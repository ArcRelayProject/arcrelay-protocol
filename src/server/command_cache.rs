use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use prost::Message;
use tokio::sync::watch;

use crate::proto_msg::proto;

const COMMAND_CACHE_TTL: Duration = Duration::from_secs(5 * 60);
const MAX_CACHED_COMMANDS_PER_DEVICE: usize = 256;
const MAX_CACHED_COMMAND_BYTES_PER_DEVICE: usize = 8 * 1024 * 1024;
const MAX_CACHED_COMMANDS_GLOBAL: usize = 1024;
const MAX_CACHED_COMMAND_BYTES_GLOBAL: usize = 64 * 1024 * 1024;

#[derive(Default)]
pub(super) struct CommandCache {
    pub entries: HashMap<CommandCacheKey, CachedCommand>,
    pub in_flight: HashMap<CommandCacheKey, InFlightCommand>,
    insertion_order: VecDeque<CommandCacheKey>,
    per_device_bytes: HashMap<Vec<u8>, usize>,
    per_device_entries: HashMap<Vec<u8>, usize>,
    total_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_device_byte_budget_evicts_oversized_entries() {
        let mut cache = CommandCache::default();
        let key = CommandCacheKey::new(&[1_u8; 32], &[2_u8; 16]);
        cache.insert(
            key,
            vec![0_u8; MAX_CACHED_COMMAND_BYTES_PER_DEVICE + 1],
            proto::Response::default(),
        );
        assert!(cache.entries.is_empty());
        assert_eq!(cache.total_bytes, 0);
    }

    #[test]
    fn one_device_cannot_evict_another_devices_entries_with_an_oversized_value() {
        let mut cache = CommandCache::default();
        let first = CommandCacheKey::new(&[1_u8; 32], &[1_u8; 16]);
        cache.insert(first.clone(), b"small".to_vec(), proto::Response::default());

        cache.insert(
            CommandCacheKey::new(&[2_u8; 32], &[2_u8; 16]),
            vec![0_u8; MAX_CACHED_COMMAND_BYTES_PER_DEVICE + 1],
            proto::Response::default(),
        );

        assert!(cache.entries.contains_key(&first));
        assert_eq!(cache.entries.len(), 1);
    }

    #[test]
    fn replacing_a_key_keeps_only_one_order_entry() {
        let mut cache = CommandCache::default();
        let key = CommandCacheKey::new(&[1_u8; 32], &[2_u8; 16]);
        cache.insert(key.clone(), b"first".to_vec(), proto::Response::default());
        cache.insert(key.clone(), b"second".to_vec(), proto::Response::default());

        assert_eq!(cache.insertion_order.len(), 1);
        assert_eq!(cache.entries.get(&key).unwrap().command, b"second");
    }

    #[test]
    fn authenticated_device_is_part_of_the_cache_key() {
        assert_ne!(
            CommandCacheKey::new(&[1_u8; 32], &[9_u8; 16]),
            CommandCacheKey::new(&[2_u8; 32], &[9_u8; 16]),
        );
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct CommandCacheKey {
    device_public_key: Vec<u8>,
    idempotency_key: Vec<u8>,
}

impl CommandCacheKey {
    pub fn new(device_public_key: &[u8], idempotency_key: &[u8]) -> Self {
        Self {
            device_public_key: device_public_key.to_vec(),
            idempotency_key: idempotency_key.to_vec(),
        }
    }
}

pub(super) struct InFlightCommand {
    pub command: Vec<u8>,
    pub result_tx: watch::Sender<Option<proto::Response>>,
}

pub(super) struct CachedCommand {
    pub command: Vec<u8>,
    pub response: proto::Response,
    created_at: Instant,
    size: usize,
}

impl CommandCache {
    pub fn prune(&mut self) {
        let now = Instant::now();
        while let Some(key) = self.insertion_order.front() {
            let remove = self
                .entries
                .get(key)
                .is_none_or(|entry| now.duration_since(entry.created_at) >= COMMAND_CACHE_TTL);
            if !remove {
                break;
            }
            if let Some(key) = self.insertion_order.pop_front() {
                self.remove_entry(&key);
            }
        }

        while self.global_limits_exceeded() {
            let Some(key) = self.insertion_order.pop_front() else {
                break;
            };
            self.remove_entry(&key);
        }
    }

    pub fn insert(&mut self, key: CommandCacheKey, command: Vec<u8>, response: proto::Response) {
        self.remove_entry(&key);
        self.insertion_order.retain(|existing| existing != &key);
        let size = key
            .idempotency_key
            .len()
            .saturating_add(command.len())
            .saturating_add(response.encoded_len());
        if size > MAX_CACHED_COMMAND_BYTES_PER_DEVICE || size > MAX_CACHED_COMMAND_BYTES_GLOBAL {
            return;
        }
        self.total_bytes = self.total_bytes.saturating_add(size);
        *self
            .per_device_bytes
            .entry(key.device_public_key.clone())
            .or_default() += size;
        *self
            .per_device_entries
            .entry(key.device_public_key.clone())
            .or_default() += 1;
        self.entries.insert(
            key.clone(),
            CachedCommand {
                command,
                response,
                created_at: Instant::now(),
                size,
            },
        );
        self.insertion_order.push_back(key);
        self.enforce_device_limits();
        self.prune();
    }

    fn global_limits_exceeded(&self) -> bool {
        self.entries.len() > MAX_CACHED_COMMANDS_GLOBAL
            || self.total_bytes > MAX_CACHED_COMMAND_BYTES_GLOBAL
    }

    fn enforce_device_limits(&mut self) {
        let over_budget = self
            .per_device_entries
            .keys()
            .filter(|device| {
                self.per_device_entries
                    .get(*device)
                    .is_some_and(|count| *count > MAX_CACHED_COMMANDS_PER_DEVICE)
                    || self
                        .per_device_bytes
                        .get(*device)
                        .is_some_and(|bytes| *bytes > MAX_CACHED_COMMAND_BYTES_PER_DEVICE)
            })
            .cloned()
            .collect::<Vec<_>>();

        for device in over_budget {
            while self
                .per_device_entries
                .get(&device)
                .is_some_and(|count| *count > MAX_CACHED_COMMANDS_PER_DEVICE)
                || self
                    .per_device_bytes
                    .get(&device)
                    .is_some_and(|bytes| *bytes > MAX_CACHED_COMMAND_BYTES_PER_DEVICE)
            {
                let Some(position) = self
                    .insertion_order
                    .iter()
                    .position(|key| key.device_public_key == device)
                else {
                    break;
                };
                if let Some(key) = self.insertion_order.remove(position) {
                    self.remove_entry(&key);
                }
            }
        }
    }

    fn remove_entry(&mut self, key: &CommandCacheKey) {
        let Some(entry) = self.entries.remove(key) else {
            return;
        };
        self.total_bytes = self.total_bytes.saturating_sub(entry.size);
        let remove_bytes =
            if let Some(bytes) = self.per_device_bytes.get_mut(&key.device_public_key) {
                *bytes = bytes.saturating_sub(entry.size);
                *bytes == 0
            } else {
                false
            };
        if remove_bytes {
            self.per_device_bytes.remove(&key.device_public_key);
        }
        let remove_count =
            if let Some(count) = self.per_device_entries.get_mut(&key.device_public_key) {
                *count = count.saturating_sub(1);
                *count == 0
            } else {
                false
            };
        if remove_count {
            self.per_device_entries.remove(&key.device_public_key);
        }
    }
}
