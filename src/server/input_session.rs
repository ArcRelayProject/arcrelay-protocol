use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use rand::RngCore;
use tokio::sync::Mutex;
use tokio::time::Instant;

use arcrelay_core::domain::input_control::{
    InputEvent as DomainInputEvent, MouseButton as DomainMouseButton, SystemGestureSequence,
};

pub const INPUT_LEASE_TIMEOUT: Duration = Duration::from_secs(5);
pub const MAX_RELIABLE_EVENTS_PER_FRAME: usize = 64;
pub const MAX_RELIABLE_FRAMES_PER_SECOND: usize = 240;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InputLease {
    pub session_id: u64,
    pub epoch: u32,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct InputFeedbackSnapshot {
    pub reliable_sequence: u64,
    pub motion_sequence: u64,
    pub motion_datagrams_received: u32,
    pub motion_datagrams_missing: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct MotionDelta {
    pub pointer_x_256: i64,
    pub pointer_y_256: i64,
    pub scroll_x_256: i64,
    pub scroll_y_256: i64,
    pub precise_scroll: bool,
    pub feedback: InputFeedbackSnapshot,
}

#[derive(Debug)]
struct ActiveInputSession {
    lease: InputLease,
    device_id: String,
    last_activity: Instant,
    reliable_sequence: u64,
    reliable_elapsed_us: u64,
    reliable_frames: VecDeque<Instant>,
    pointer_total_x_256: i64,
    pointer_total_y_256: i64,
    scroll_total_x_256: i64,
    scroll_total_y_256: i64,
    pressed_keys: HashSet<u16>,
    pressed_buttons: HashSet<DomainMouseButton>,
    system_gesture: SystemGestureSequence,
    system_gesture_updated: Option<Instant>,
    workspace_routed: bool,
}

#[derive(Debug, Default)]
struct InputSessionState {
    active: HashMap<InputLease, ActiveInputSession>,
    pressed_key_counts: HashMap<u16, usize>,
    pressed_button_counts: HashMap<DomainMouseButton, usize>,
}

#[derive(Debug, Clone, Default)]
pub struct InputSessionManager {
    state: Arc<Mutex<InputSessionState>>,
    apply_gate: Arc<Mutex<()>>,
}

impl InputSessionManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Serializes native input application and shared pressed-state updates
    /// across every connected client.
    pub fn apply_gate(&self) -> Arc<Mutex<()>> {
        self.apply_gate.clone()
    }

    #[cfg(test)]
    pub async fn acquire(
        &self,
        device_id: &str,
        device_name: &str,
    ) -> std::result::Result<InputLease, String> {
        self.acquire_with_routing(device_id, device_name, false)
            .await
    }

    pub async fn acquire_with_routing(
        &self,
        device_id: &str,
        _device_name: &str,
        workspace_routed: bool,
    ) -> std::result::Result<InputLease, String> {
        let mut state = self.state.lock().await;
        if let Some(active) = state.active.values().next() {
            return Err(format!(
                "input is already controlled by {} at epoch {}",
                active.device_id, active.lease.epoch
            ));
        }
        let mut random = rand::rngs::OsRng;
        // Keep identifiers in the positive signed 63-bit range so every
        // supported client runtime can represent them without lossy casts.
        let lease = loop {
            let mut session_id = random.next_u64() & i64::MAX as u64;
            if session_id == 0 {
                session_id = 1;
            }
            let mut epoch = random.next_u32();
            if epoch == 0 {
                epoch = 1;
            }
            let lease = InputLease { session_id, epoch };
            if !state.active.contains_key(&lease) {
                break lease;
            }
        };
        state.active.insert(
            lease,
            ActiveInputSession {
                lease,
                device_id: device_id.to_string(),
                last_activity: Instant::now(),
                reliable_sequence: 0,
                reliable_elapsed_us: 0,
                reliable_frames: VecDeque::new(),
                pointer_total_x_256: 0,
                pointer_total_y_256: 0,
                scroll_total_x_256: 0,
                scroll_total_y_256: 0,
                pressed_keys: HashSet::new(),
                pressed_buttons: HashSet::new(),
                system_gesture: SystemGestureSequence::default(),
                system_gesture_updated: None,
                workspace_routed,
            },
        );
        Ok(lease)
    }

    pub async fn is_workspace_routed(&self, lease: InputLease, device_id: &str) -> bool {
        self.state
            .lock()
            .await
            .active
            .get(&lease)
            .is_some_and(|active| active.device_id == device_id && active.workspace_routed)
    }

    pub async fn accept_reliable(
        &self,
        lease: InputLease,
        device_id: &str,
        sequence: u64,
        elapsed_us: u64,
        _latest_motion_sequence: u64,
    ) -> std::result::Result<InputFeedbackSnapshot, String> {
        let mut state = self.state.lock().await;
        let current = state
            .active
            .get_mut(&lease)
            .ok_or_else(|| "input session is not active".to_string())?;
        validate_owner(current, lease, device_id)?;
        if current.last_activity.elapsed() >= INPUT_LEASE_TIMEOUT {
            return Err("input lease expired".into());
        }
        if sequence != current.reliable_sequence.saturating_add(1) {
            return Err("out-of-order reliable input frame".into());
        }
        if elapsed_us < current.reliable_elapsed_us {
            return Err("reliable input clock moved backwards".into());
        }
        let now = Instant::now();
        while current
            .reliable_frames
            .front()
            .is_some_and(|timestamp| now.duration_since(*timestamp) >= Duration::from_secs(1))
        {
            current.reliable_frames.pop_front();
        }
        if current.reliable_frames.len() >= MAX_RELIABLE_FRAMES_PER_SECOND {
            return Err("reliable input frame rate limit exceeded".into());
        }
        current.reliable_frames.push_back(now);
        current.reliable_sequence = sequence;
        current.reliable_elapsed_us = elapsed_us;
        current.last_activity = now;
        Ok(feedback_snapshot(current))
    }

    pub async fn accept_ordered_motion(
        &self,
        lease: InputLease,
        device_id: &str,
        pointer_total_x_256: i64,
        pointer_total_y_256: i64,
        scroll_total_x_256: i64,
        scroll_total_y_256: i64,
        precise_scroll: bool,
    ) -> std::result::Result<MotionDelta, String> {
        let mut state = self.state.lock().await;
        let current = state
            .active
            .get_mut(&lease)
            .ok_or_else(|| "input session is not active".to_string())?;
        validate_owner(current, lease, device_id)?;
        if current.last_activity.elapsed() >= INPUT_LEASE_TIMEOUT {
            return Err("input lease expired".into());
        }

        let pointer_x_256 = pointer_total_x_256
            .checked_sub(current.pointer_total_x_256)
            .ok_or_else(|| "pointer total overflow".to_string())?;
        let pointer_y_256 = pointer_total_y_256
            .checked_sub(current.pointer_total_y_256)
            .ok_or_else(|| "pointer total overflow".to_string())?;
        let scroll_x_256 = scroll_total_x_256
            .checked_sub(current.scroll_total_x_256)
            .ok_or_else(|| "scroll total overflow".to_string())?;
        let scroll_y_256 = scroll_total_y_256
            .checked_sub(current.scroll_total_y_256)
            .ok_or_else(|| "scroll total overflow".to_string())?;
        if pointer_x_256.unsigned_abs() > 8192 * 256
            || pointer_y_256.unsigned_abs() > 8192 * 256
            || scroll_x_256.unsigned_abs() > 2048 * 256
            || scroll_y_256.unsigned_abs() > 2048 * 256
        {
            return Err("motion delta exceeds protocol limits".into());
        }

        current.pointer_total_x_256 = pointer_total_x_256;
        current.pointer_total_y_256 = pointer_total_y_256;
        current.scroll_total_x_256 = scroll_total_x_256;
        current.scroll_total_y_256 = scroll_total_y_256;
        current.last_activity = Instant::now();
        Ok(MotionDelta {
            pointer_x_256,
            pointer_y_256,
            scroll_x_256,
            scroll_y_256,
            precise_scroll,
            feedback: feedback_snapshot(current),
        })
    }

    pub async fn is_expired(&self, lease: InputLease, device_id: &str) -> bool {
        let state = self.state.lock().await;
        state.active.get(&lease).is_some_and(|current| {
            current.device_id == device_id && current.last_activity.elapsed() >= INPUT_LEASE_TIMEOUT
        })
    }

    /// Converts per-client key/button state into the effective native events.
    /// A key held by multiple clients remains down until the final holder
    /// releases it. `ReleaseAll` only releases state owned by this lease.
    pub async fn normalize_reliable_events(
        &self,
        lease: InputLease,
        device_id: &str,
        events: Vec<DomainInputEvent>,
    ) -> std::result::Result<Vec<DomainInputEvent>, String> {
        for event in &events {
            if let DomainInputEvent::SystemGesture(gesture) = event {
                gesture.validate().map_err(str::to_string)?;
            }
        }
        let mut state = self.state.lock().await;
        let mut current = state
            .active
            .remove(&lease)
            .ok_or_else(|| "input session is not active".to_string())?;
        if let Err(error) = validate_owner(&current, lease, device_id) {
            state.active.insert(lease, current);
            return Err(error);
        }

        let mut normalized = Vec::with_capacity(events.len());
        for event in events {
            match event {
                DomainInputEvent::Key {
                    hid_usage,
                    down,
                    repeat,
                } => {
                    if down {
                        if current.pressed_keys.insert(hid_usage) {
                            let count = state.pressed_key_counts.entry(hid_usage).or_default();
                            *count += 1;
                            if *count == 1 {
                                normalized.push(DomainInputEvent::Key {
                                    hid_usage,
                                    down,
                                    repeat,
                                });
                            }
                        } else if repeat {
                            normalized.push(DomainInputEvent::Key {
                                hid_usage,
                                down,
                                repeat,
                            });
                        }
                    } else if current.pressed_keys.remove(&hid_usage)
                        && decrement_count(&mut state.pressed_key_counts, hid_usage)
                    {
                        normalized.push(DomainInputEvent::Key {
                            hid_usage,
                            down,
                            repeat,
                        });
                    }
                }
                DomainInputEvent::PointerButton {
                    button,
                    down,
                    click_count,
                } => {
                    if down {
                        if current.pressed_buttons.insert(button) {
                            let count = state.pressed_button_counts.entry(button).or_default();
                            *count += 1;
                            if *count == 1 {
                                normalized.push(DomainInputEvent::PointerButton {
                                    button,
                                    down,
                                    click_count,
                                });
                            }
                        }
                    } else if current.pressed_buttons.remove(&button)
                        && decrement_count(&mut state.pressed_button_counts, button)
                    {
                        normalized.push(DomainInputEvent::PointerButton {
                            button,
                            down,
                            click_count,
                        });
                    }
                }
                DomainInputEvent::ReleaseAll => {
                    normalized.extend(release_session_state(&mut state, &mut current));
                }
                DomainInputEvent::SystemGesture(gesture) => {
                    let phases = current
                        .system_gesture
                        .apply(gesture)
                        .expect("system gesture batch was validated before normalization");
                    if !phases.is_empty() {
                        current.system_gesture_updated =
                            current.system_gesture.is_active().then(Instant::now);
                    }
                    normalized.extend(phases.into_iter().map(DomainInputEvent::SystemGesture));
                }
                other => normalized.push(other),
            }
        }
        state.active.insert(lease, current);
        Ok(normalized)
    }

    /// Keepalives renew the lease, but must not keep an abandoned DockSwipe alive.
    pub async fn cancel_idle_system_gesture(
        &self,
        lease: InputLease,
        device_id: &str,
    ) -> Vec<DomainInputEvent> {
        let mut state = self.state.lock().await;
        let Some(current) = state.active.get_mut(&lease) else {
            return Vec::new();
        };
        if validate_owner(current, lease, device_id).is_err()
            || !current
                .system_gesture_updated
                .is_some_and(|updated| updated.elapsed() >= Duration::from_secs(2))
        {
            return Vec::new();
        }
        current.system_gesture_updated = None;
        current
            .system_gesture
            .cancel()
            .map(DomainInputEvent::SystemGesture)
            .into_iter()
            .collect()
    }

    /// Removes one client session and returns only the native key/button-up
    /// events that are no longer held by any other client.
    pub async fn release(&self, lease: InputLease) -> Vec<DomainInputEvent> {
        let mut state = self.state.lock().await;
        let Some(mut current) = state.active.remove(&lease) else {
            return Vec::new();
        };
        release_session_state(&mut state, &mut current)
    }
}

fn decrement_count<T: Eq + std::hash::Hash + Copy>(
    counts: &mut HashMap<T, usize>,
    value: T,
) -> bool {
    let Some(count) = counts.get_mut(&value) else {
        return false;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        counts.remove(&value);
        true
    } else {
        false
    }
}

fn release_session_state(
    state: &mut InputSessionState,
    current: &mut ActiveInputSession,
) -> Vec<DomainInputEvent> {
    let mut releases =
        Vec::with_capacity(current.pressed_keys.len() + current.pressed_buttons.len());
    current.system_gesture_updated = None;
    releases.extend(
        current
            .system_gesture
            .cancel()
            .map(DomainInputEvent::SystemGesture),
    );
    for hid_usage in current.pressed_keys.drain() {
        if decrement_count(&mut state.pressed_key_counts, hid_usage) {
            releases.push(DomainInputEvent::Key {
                hid_usage,
                down: false,
                repeat: false,
            });
        }
    }
    for button in current.pressed_buttons.drain() {
        if decrement_count(&mut state.pressed_button_counts, button) {
            releases.push(DomainInputEvent::PointerButton {
                button,
                down: false,
                click_count: 1,
            });
        }
    }
    releases
}

fn feedback_snapshot(current: &ActiveInputSession) -> InputFeedbackSnapshot {
    InputFeedbackSnapshot {
        reliable_sequence: current.reliable_sequence,
        ..InputFeedbackSnapshot::default()
    }
}

fn validate_owner(
    current: &ActiveInputSession,
    lease: InputLease,
    device_id: &str,
) -> std::result::Result<(), String> {
    if current.lease != lease || current.device_id != device_id {
        Err("input session identity mismatch".into())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn workspace_routing_is_bound_to_the_authenticated_lease() {
        let manager = InputSessionManager::new();
        let lease = manager
            .acquire_with_routing("phone", "Phone", true)
            .await
            .unwrap();

        assert!(manager.is_workspace_routed(lease, "phone").await);
        assert!(!manager.is_workspace_routed(lease, "tablet").await);
        manager.release(lease).await;
        assert!(!manager.is_workspace_routed(lease, "phone").await);
    }

    fn swipe(phase: u32) -> DomainInputEvent {
        DomainInputEvent::SystemGesture(arcrelay_core::domain::input_control::SystemGestureEvent {
            axis: 1,
            phase,
            progress: -0.4,
            velocity_x: 0.0,
            velocity_y: 0.0,
            inverted_from_device: false,
        })
    }

    #[tokio::test]
    async fn dock_swipe_lease_release_and_replacement_cancel_but_tails_do_not_start() {
        let manager = InputSessionManager::new();
        let lease = manager.acquire("phone", "Phone").await.unwrap();
        assert!(manager
            .normalize_reliable_events(lease, "other", vec![swipe(1)])
            .await
            .is_err());
        assert!(manager
            .normalize_reliable_events(lease, "phone", vec![swipe(2)])
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            manager
                .normalize_reliable_events(lease, "phone", vec![swipe(1)])
                .await
                .unwrap(),
            vec![swipe(1)]
        );
        assert_eq!(
            manager
                .normalize_reliable_events(lease, "phone", vec![swipe(1)])
                .await
                .unwrap(),
            vec![swipe(8), swipe(1)]
        );
        assert_eq!(
            manager
                .normalize_reliable_events(lease, "phone", vec![DomainInputEvent::ReleaseAll])
                .await
                .unwrap(),
            vec![swipe(8)]
        );
        manager
            .normalize_reliable_events(lease, "phone", vec![swipe(1)])
            .await
            .unwrap();
        assert_eq!(manager.release(lease).await, vec![swipe(8)]);
        let next = manager.acquire("phone", "Phone").await.unwrap();
        assert!(manager
            .normalize_reliable_events(lease, "phone", vec![swipe(2)])
            .await
            .is_err());
        assert!(manager
            .normalize_reliable_events(next, "phone", vec![swipe(4)])
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn dock_swipe_timeout_ignores_keepalive_and_is_owner_scoped() {
        let manager = InputSessionManager::new();
        let lease = manager.acquire("phone", "Phone").await.unwrap();
        manager
            .normalize_reliable_events(lease, "phone", vec![swipe(1)])
            .await
            .unwrap();
        manager
            .state
            .lock()
            .await
            .active
            .get_mut(&lease)
            .unwrap()
            .system_gesture_updated = Some(Instant::now() - Duration::from_secs(3));
        manager
            .accept_reliable(lease, "phone", 1, 1, 0)
            .await
            .unwrap();
        assert!(manager
            .cancel_idle_system_gesture(lease, "other")
            .await
            .is_empty());
        assert_eq!(
            manager.cancel_idle_system_gesture(lease, "phone").await,
            vec![swipe(8)]
        );
        assert!(manager
            .cancel_idle_system_gesture(lease, "phone")
            .await
            .is_empty());
        assert!(manager
            .normalize_reliable_events(lease, "phone", vec![swipe(2)])
            .await
            .unwrap()
            .is_empty());
    }

    #[test]
    fn system_gesture_session_construction_needs_no_tokio_runtime() {
        let manager = InputSessionManager::new();
        assert!(manager.state.try_lock().unwrap().active.is_empty());
    }

    fn key(hid_usage: u16, down: bool) -> DomainInputEvent {
        DomainInputEvent::Key {
            hid_usage,
            down,
            repeat: false,
        }
    }

    #[tokio::test]
    async fn only_one_device_can_hold_the_global_input_session() {
        let manager = InputSessionManager::new();
        let phone = manager.acquire("phone", "Phone").await.unwrap();
        assert!(manager.acquire("tablet", "Tablet").await.is_err());

        manager
            .accept_reliable(phone, "phone", 1, 1, 0)
            .await
            .unwrap();
        let phone_motion = manager
            .accept_ordered_motion(phone, "phone", 256, 0, 0, 0, true)
            .await
            .unwrap();
        assert_eq!(phone_motion.pointer_x_256, 256);
        manager.release(phone).await;
        assert!(manager.acquire("tablet", "Tablet").await.is_ok());
    }

    #[tokio::test]
    async fn release_clears_held_key_before_next_controller() {
        let manager = InputSessionManager::new();
        let phone = manager.acquire("phone", "Phone").await.unwrap();

        assert_eq!(
            manager
                .normalize_reliable_events(phone, "phone", vec![key(0x04, true)])
                .await
                .unwrap(),
            vec![key(0x04, true)]
        );
        assert_eq!(manager.release(phone).await, vec![key(0x04, false)]);
        let tablet = manager.acquire("tablet", "Tablet").await.unwrap();
        assert_eq!(
            manager
                .normalize_reliable_events(tablet, "tablet", vec![key(0x04, true)])
                .await
                .unwrap(),
            vec![key(0x04, true)]
        );
    }

    #[tokio::test]
    async fn release_all_clears_the_global_button_state() {
        let manager = InputSessionManager::new();
        let phone = manager.acquire("phone", "Phone").await.unwrap();
        let button_down = DomainInputEvent::PointerButton {
            button: DomainMouseButton::Left,
            down: true,
            click_count: 1,
        };

        assert_eq!(
            manager
                .normalize_reliable_events(phone, "phone", vec![button_down.clone()])
                .await
                .unwrap(),
            vec![button_down.clone()]
        );
        assert_eq!(
            manager
                .normalize_reliable_events(phone, "phone", vec![DomainInputEvent::ReleaseAll])
                .await
                .unwrap(),
            vec![DomainInputEvent::PointerButton {
                button: DomainMouseButton::Left,
                down: false,
                click_count: 1,
            }]
        );
        assert!(manager.release(phone).await.is_empty());
    }

    #[tokio::test]
    async fn lease_is_bound_to_authenticated_device() {
        let manager = InputSessionManager::new();
        let lease = manager.acquire("device-a", "Phone").await.unwrap();

        assert!(manager
            .accept_reliable(lease, "device-b", 1, 1, 0)
            .await
            .is_err());
        assert!(manager
            .accept_reliable(lease, "device-a", 1, 1, 0)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn ordered_motion_uses_latest_cumulative_totals_without_catch_up_jump() {
        let manager = InputSessionManager::new();
        let lease = manager.acquire("device", "iPad").await.unwrap();

        manager
            .accept_reliable(lease, "device", 1, 1, 0)
            .await
            .unwrap();
        let first = manager
            .accept_ordered_motion(lease, "device", 384, -128, 0, 0, true)
            .await
            .unwrap();
        assert_eq!(first.pointer_x_256, 384);
        assert_eq!(first.pointer_y_256, -128);

        manager
            .accept_reliable(lease, "device", 2, 2, 0)
            .await
            .unwrap();
        let next = manager
            .accept_ordered_motion(lease, "device", 512, -64, 0, 0, true)
            .await
            .unwrap();
        assert_eq!(next.pointer_x_256, 128);
        assert_eq!(next.pointer_y_256, 64);
    }

    #[tokio::test]
    async fn reliable_keepalives_are_rate_limited() {
        let manager = InputSessionManager::new();
        let lease = manager.acquire("device", "Phone").await.unwrap();

        for sequence in 1..=MAX_RELIABLE_FRAMES_PER_SECOND as u64 {
            manager
                .accept_reliable(lease, "device", sequence, sequence, 0)
                .await
                .unwrap();
        }
        assert!(manager
            .accept_reliable(
                lease,
                "device",
                MAX_RELIABLE_FRAMES_PER_SECOND as u64 + 1,
                MAX_RELIABLE_FRAMES_PER_SECOND as u64 + 1,
                0,
            )
            .await
            .is_err());
    }
}
