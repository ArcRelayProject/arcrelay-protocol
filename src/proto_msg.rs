//! Canonical protobuf types and explicit domain adapters.
//!
//! The generated Rust contract lives in `arcrelay-wire`. Keeping adapters as
//! named functions makes the domain/wire boundary visible and avoids orphan
//! implementations when both sides are owned by other crates.

pub use arcrelay_wire::proto;
pub use proto::{ClientControlFrame, ServerControlFrame};

use arcrelay_core::domain::{
    clipboard::{ClipboardContentKind, ClipboardLabel, ClipboardPolicy, ClipboardSummary},
    device::{ConnectionConfig, DeviceType, LocalDeviceInfo},
    media_control::{AppVolume, PlaybackAction, PlaybackInfo, VolumeInfo},
    process::{ProcessInfo, ProcessSortBy},
    system_monitor::{CpuInfo, DiskInfo, GpuInfo, MemoryInfo, NetworkStats, SystemSnapshot},
    window_manager::{SpaceInfo, WindowInfo},
};

#[must_use]
pub fn system_snapshot_to_proto(snapshot: SystemSnapshot) -> proto::SystemSnapshot {
    proto::SystemSnapshot {
        cpu: Some(cpu_info_to_proto(snapshot.cpu)),
        memory: Some(memory_info_to_proto(snapshot.memory)),
        gpus: snapshot.gpus.into_iter().map(gpu_info_to_proto).collect(),
        disks: snapshot.disks.into_iter().map(disk_info_to_proto).collect(),
        network: Some(network_stats_to_proto(snapshot.network)),
    }
}

#[must_use]
pub fn cpu_info_to_proto(cpu: CpuInfo) -> proto::CpuInfo {
    proto::CpuInfo {
        usage_percent: cpu.usage_percent,
        core_count: cpu.core_count,
        temperature_celsius: cpu.temperature_celsius,
        model_name: cpu.model_name,
    }
}

#[must_use]
pub fn memory_info_to_proto(memory: MemoryInfo) -> proto::MemoryInfo {
    proto::MemoryInfo {
        used_bytes: memory.used_bytes,
        total_bytes: memory.total_bytes,
        usage_percent: memory.usage_percent,
    }
}

#[must_use]
pub fn gpu_info_to_proto(gpu: GpuInfo) -> proto::GpuInfo {
    proto::GpuInfo {
        name: gpu.name,
        usage_percent: gpu.usage_percent,
        temperature_celsius: gpu.temperature_celsius,
    }
}

#[must_use]
pub fn disk_info_to_proto(disk: DiskInfo) -> proto::DiskInfo {
    proto::DiskInfo {
        name: disk.name,
        used_bytes: disk.used_bytes,
        total_bytes: disk.total_bytes,
        usage_percent: disk.usage_percent,
    }
}

#[must_use]
pub fn network_stats_to_proto(network: NetworkStats) -> proto::NetworkStats {
    proto::NetworkStats {
        download_bytes_per_sec: network.download_bytes_per_sec,
        upload_bytes_per_sec: network.upload_bytes_per_sec,
    }
}

#[must_use]
pub fn process_info_to_proto(process: ProcessInfo) -> proto::ProcessInfo {
    proto::ProcessInfo {
        pid: process.pid,
        name: process.name,
        cpu_percent: process.cpu_percent,
        memory_bytes: process.memory_bytes,
    }
}

#[must_use]
pub const fn process_sort_to_proto(sort: ProcessSortBy) -> proto::ProcessSortBy {
    match sort {
        ProcessSortBy::Cpu => proto::ProcessSortBy::Cpu,
        ProcessSortBy::Memory => proto::ProcessSortBy::Memory,
        ProcessSortBy::Name => proto::ProcessSortBy::Name,
    }
}

#[must_use]
pub const fn process_sort_from_proto(sort: proto::ProcessSortBy) -> ProcessSortBy {
    match sort {
        proto::ProcessSortBy::Unspecified | proto::ProcessSortBy::Cpu => ProcessSortBy::Cpu,
        proto::ProcessSortBy::Memory => ProcessSortBy::Memory,
        proto::ProcessSortBy::Name => ProcessSortBy::Name,
    }
}

#[must_use]
pub fn playback_info_to_proto(playback: PlaybackInfo) -> proto::PlaybackInfo {
    proto::PlaybackInfo {
        title: playback.title,
        artist: playback.artist,
        source_app: playback.source_app,
        position_secs: playback.position_secs,
        duration_secs: playback.duration_secs,
        is_playing: playback.is_playing,
        confidence: proto::MediaConfidence::Authoritative as i32,
    }
}

#[must_use]
pub fn volume_info_to_proto(volume: VolumeInfo) -> proto::VolumeInfo {
    proto::VolumeInfo {
        system_volume: u32::from(volume.system_volume),
        is_muted: volume.is_muted,
    }
}

#[must_use]
pub fn app_volume_to_proto(volume: AppVolume) -> proto::AppVolume {
    proto::AppVolume {
        app_name: volume.app_name,
        volume: u32::from(volume.volume),
    }
}

#[must_use]
pub const fn playback_action_to_proto(action: PlaybackAction) -> proto::PlaybackAction {
    match action {
        PlaybackAction::Play => proto::PlaybackAction::Play,
        PlaybackAction::Pause => proto::PlaybackAction::Pause,
        PlaybackAction::Next => proto::PlaybackAction::Next,
        PlaybackAction::Previous => proto::PlaybackAction::Previous,
        PlaybackAction::SeekForward => proto::PlaybackAction::SeekForward,
        PlaybackAction::SeekBackward => proto::PlaybackAction::SeekBackward,
    }
}

#[must_use]
pub const fn playback_action_from_proto(action: proto::PlaybackAction) -> PlaybackAction {
    match action {
        proto::PlaybackAction::Unspecified | proto::PlaybackAction::Play => PlaybackAction::Play,
        proto::PlaybackAction::Pause => PlaybackAction::Pause,
        proto::PlaybackAction::Next => PlaybackAction::Next,
        proto::PlaybackAction::Previous => PlaybackAction::Previous,
        proto::PlaybackAction::SeekForward => PlaybackAction::SeekForward,
        proto::PlaybackAction::SeekBackward => PlaybackAction::SeekBackward,
    }
}

#[must_use]
pub const fn clipboard_kind_to_proto(kind: ClipboardContentKind) -> proto::ClipboardContentKind {
    match kind {
        ClipboardContentKind::Text => proto::ClipboardContentKind::Text,
        ClipboardContentKind::Html => proto::ClipboardContentKind::Html,
        ClipboardContentKind::Image => proto::ClipboardContentKind::Image,
        ClipboardContentKind::Files => proto::ClipboardContentKind::Files,
    }
}

#[must_use]
pub fn clipboard_summary_to_proto(summary: ClipboardSummary) -> proto::ClipboardEntry {
    proto::ClipboardEntry {
        id: summary.id,
        kind: clipboard_kind_to_proto(summary.kind) as i32,
        preview: summary.preview,
        source_app: summary.source_app,
        timestamp_ms: summary.captured_at.timestamp_millis(),
        size_bytes: summary.size_bytes,
        item_count: summary.item_count,
        width: summary.width,
        height: summary.height,
        sensitive: summary.sensitive,
        favorite: summary.favorite,
        copy_count: summary.copy_count,
        available: summary.available,
        labels: summary
            .labels
            .into_iter()
            .map(clipboard_label_to_proto)
            .collect(),
    }
}

#[must_use]
pub fn clipboard_label_to_proto(label: ClipboardLabel) -> proto::ClipboardLabel {
    proto::ClipboardLabel {
        id: label.id,
        name: label.name,
        color: label.color,
        revision: label.revision,
        updated_by_device_id: label.updated_by_device_id,
        deleted: label.deleted,
    }
}

#[must_use]
pub fn clipboard_policy_to_proto(policy: ClipboardPolicy) -> proto::ClipboardPolicy {
    proto::ClipboardPolicy {
        history_enabled: policy.history_enabled,
        max_items: policy.max_items,
        max_bytes: policy.max_bytes,
        retention_days: policy.retention_days,
        save_sensitive: policy.save_sensitive,
    }
}

#[must_use]
pub const fn device_type_to_proto(device_type: DeviceType) -> proto::DeviceType {
    match device_type {
        DeviceType::MacBook => proto::DeviceType::Macbook,
        DeviceType::WindowsPC => proto::DeviceType::WindowsPc,
        DeviceType::LinuxServer => proto::DeviceType::LinuxServer,
        DeviceType::IPad => proto::DeviceType::Ipad,
        DeviceType::IPhone => proto::DeviceType::Iphone,
        DeviceType::Android => proto::DeviceType::Android,
        DeviceType::Unknown => proto::DeviceType::Unspecified,
    }
}

#[must_use]
pub fn local_device_info_to_proto(device: LocalDeviceInfo) -> proto::LocalDeviceInfo {
    proto::LocalDeviceInfo {
        name: device.name,
        ip: device.ip,
        app_version: device.app_version,
        port: u32::from(device.port),
    }
}

#[must_use]
pub fn connection_config_to_proto(config: ConnectionConfig) -> proto::ConnectionConfig {
    proto::ConnectionConfig {
        port: u32::from(config.port),
        encrypted: config.encrypted,
        auto_reconnect: config.auto_reconnect,
    }
}

#[must_use]
pub fn window_info_to_proto(window: WindowInfo) -> proto::WindowInfo {
    proto::WindowInfo {
        window_id: window.window_id,
        title: window.title,
        app_name: window.app_name,
        is_focused: window.is_focused,
        thumbnail: None,
        space_id: window.space_id,
    }
}

#[must_use]
pub fn space_info_to_proto(space: SpaceInfo) -> proto::SpaceInfo {
    proto::SpaceInfo {
        space_id: space.space_id,
        label: space.label,
        is_active: space.is_active,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_snapshot_conversion_preserves_nested_values() {
        let converted = system_snapshot_to_proto(SystemSnapshot {
            cpu: CpuInfo {
                usage_percent: 12.5,
                core_count: 8,
                temperature_celsius: Some(61.0),
                model_name: "Test CPU".into(),
            },
            memory: MemoryInfo {
                used_bytes: 40,
                total_bytes: 100,
                usage_percent: 40.0,
            },
            gpus: vec![GpuInfo {
                name: "GPU".into(),
                usage_percent: Some(30.0),
                temperature_celsius: None,
            }],
            disks: vec![DiskInfo {
                name: "/".into(),
                used_bytes: 70,
                total_bytes: 100,
                usage_percent: 70.0,
            }],
            network: NetworkStats {
                download_bytes_per_sec: 123,
                upload_bytes_per_sec: 45,
            },
        });

        let cpu = converted.cpu.unwrap();
        assert_eq!((cpu.model_name.as_str(), cpu.core_count), ("Test CPU", 8));
        assert_eq!(converted.memory.unwrap().used_bytes, 40);
        assert_eq!(converted.gpus[0].name, "GPU");
        assert_eq!(converted.disks[0].usage_percent, 70.0);
        assert_eq!(converted.network.unwrap().download_bytes_per_sec, 123);
    }

    #[test]
    fn process_sort_conversion_covers_defaults_and_round_trips() {
        assert!(matches!(
            process_sort_from_proto(proto::ProcessSortBy::Unspecified),
            ProcessSortBy::Cpu
        ));
        for (domain, wire) in [
            (ProcessSortBy::Cpu, proto::ProcessSortBy::Cpu),
            (ProcessSortBy::Memory, proto::ProcessSortBy::Memory),
            (ProcessSortBy::Name, proto::ProcessSortBy::Name),
        ] {
            assert_eq!(process_sort_to_proto(domain), wire);
            assert!(matches!(
                (process_sort_from_proto(wire), domain),
                (ProcessSortBy::Cpu, ProcessSortBy::Cpu)
                    | (ProcessSortBy::Memory, ProcessSortBy::Memory)
                    | (ProcessSortBy::Name, ProcessSortBy::Name)
            ));
        }
        let process = process_info_to_proto(ProcessInfo {
            pid: 42,
            name: "worker".into(),
            cpu_percent: 1.5,
            memory_bytes: 2048,
        });
        assert_eq!(
            (process.pid, process.name.as_str(), process.memory_bytes),
            (42, "worker", 2048)
        );
    }

    #[test]
    fn media_conversions_cover_all_actions_and_values() {
        assert!(matches!(
            playback_action_from_proto(proto::PlaybackAction::Unspecified),
            PlaybackAction::Play
        ));
        for (domain, wire) in [
            (PlaybackAction::Play, proto::PlaybackAction::Play),
            (PlaybackAction::Pause, proto::PlaybackAction::Pause),
            (PlaybackAction::Next, proto::PlaybackAction::Next),
            (PlaybackAction::Previous, proto::PlaybackAction::Previous),
            (
                PlaybackAction::SeekForward,
                proto::PlaybackAction::SeekForward,
            ),
            (
                PlaybackAction::SeekBackward,
                proto::PlaybackAction::SeekBackward,
            ),
        ] {
            assert_eq!(playback_action_to_proto(domain), wire);
            assert_eq!(
                playback_action_to_proto(playback_action_from_proto(wire)),
                wire
            );
        }
        let playback = playback_info_to_proto(PlaybackInfo {
            title: "Song".into(),
            artist: "Artist".into(),
            source_app: "Player".into(),
            position_secs: 2.5,
            duration_secs: 10.0,
            is_playing: true,
        });
        assert_eq!(
            playback.confidence,
            proto::MediaConfidence::Authoritative as i32
        );
        assert_eq!(playback.title, "Song");
        assert_eq!(
            volume_info_to_proto(VolumeInfo {
                system_volume: 73,
                is_muted: true
            })
            .system_volume,
            73
        );
        assert_eq!(
            app_volume_to_proto(AppVolume {
                app_name: "Player".into(),
                volume: 64
            })
            .volume,
            64
        );
    }

    #[test]
    fn clipboard_conversions_preserve_contract_fields() {
        for (domain, wire) in [
            (
                ClipboardContentKind::Text,
                proto::ClipboardContentKind::Text,
            ),
            (
                ClipboardContentKind::Html,
                proto::ClipboardContentKind::Html,
            ),
            (
                ClipboardContentKind::Image,
                proto::ClipboardContentKind::Image,
            ),
            (
                ClipboardContentKind::Files,
                proto::ClipboardContentKind::Files,
            ),
        ] {
            assert_eq!(clipboard_kind_to_proto(domain), wire);
        }
        let label = clipboard_label_to_proto(ClipboardLabel {
            id: "important".into(),
            name: "Important".into(),
            color: "red".into(),
            revision: 4,
            updated_by_device_id: "device".into(),
            deleted: true,
        });
        assert_eq!(
            (label.id.as_str(), label.revision, label.deleted),
            ("important", 4, true)
        );

        let policy = clipboard_policy_to_proto(ClipboardPolicy {
            history_enabled: false,
            max_items: 12,
            max_bytes: 345,
            retention_days: 6,
            save_sensitive: true,
        });
        assert_eq!(
            (policy.history_enabled, policy.max_items, policy.max_bytes),
            (false, 12, 345)
        );
        assert!(policy.save_sensitive);
    }

    #[test]
    fn device_and_window_conversions_cover_every_variant() {
        for (domain, wire) in [
            (DeviceType::MacBook, proto::DeviceType::Macbook),
            (DeviceType::WindowsPC, proto::DeviceType::WindowsPc),
            (DeviceType::LinuxServer, proto::DeviceType::LinuxServer),
            (DeviceType::IPad, proto::DeviceType::Ipad),
            (DeviceType::IPhone, proto::DeviceType::Iphone),
            (DeviceType::Android, proto::DeviceType::Android),
            (DeviceType::Unknown, proto::DeviceType::Unspecified),
        ] {
            assert_eq!(device_type_to_proto(domain), wire);
        }
        let device = local_device_info_to_proto(LocalDeviceInfo {
            name: "Mac".into(),
            ip: "127.0.0.1".into(),
            app_version: "1.2.3".into(),
            port: 8765,
        });
        assert_eq!((device.name.as_str(), device.port), ("Mac", 8765));
        let config = connection_config_to_proto(ConnectionConfig {
            port: 9999,
            encrypted: true,
            auto_reconnect: false,
        });
        assert_eq!(
            (config.port, config.encrypted, config.auto_reconnect),
            (9999, true, false)
        );

        let window = window_info_to_proto(WindowInfo {
            window_id: 9,
            title: "Editor".into(),
            app_name: "IDE".into(),
            is_focused: true,
            thumbnail_png: vec![1, 2, 3],
            space_id: 2,
        });
        assert_eq!((window.window_id, window.space_id), (9, 2));
        assert!(window.thumbnail.is_none());
        let space = space_info_to_proto(SpaceInfo {
            space_id: 2,
            label: "Desktop 2".into(),
            is_active: true,
        });
        assert_eq!((space.label.as_str(), space.is_active), ("Desktop 2", true));
    }
}
