#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_focus_notification_preserves_owner_and_route() {
        let lease = InputLease {
            session_id: 41,
            epoch: 7,
        };
        let snapshot = WorkspaceInputSnapshot {
            owner_device_id: Some("phone".into()),
            controller_device_id: "gateway".into(),
            logical_target_device_id: "target".into(),
            target_display_id: "display".into(),
            control_epoch: 12,
            supports_system_gestures: true,
            state: WorkspaceInputRouteState::Suspended,
            cause: WorkspaceInputFocusCause::PhysicalActivity,
            message: "physical takeover".into(),
        };

        let frame = input_focus_changed_frame(Some(lease), &snapshot);
        let Some(proto::server_control_frame::Body::InputFocusChanged(focus)) = frame.body else {
            panic!("expected workspace focus notification");
        };
        assert_eq!(focus.session_id, 41);
        assert_eq!(focus.epoch, 7);
        assert_eq!(focus.state, proto::InputRouteState::Suspended as i32);
        assert_eq!(
            focus.cause,
            proto::InputFocusCause::PhysicalActivity as i32
        );
        assert_eq!(focus.controller_device_id, "gateway");
        assert_eq!(focus.logical_target_device_id, "target");
        assert_eq!(focus.target_display_id, "display");
        assert_eq!(focus.control_epoch, 12);
        assert!(focus.supports_system_gestures);
    }

    #[test]
    fn dock_swipe_wire_values_are_bounded_and_old_receivers_default_off() {
        let gesture = proto::SystemGesture { format_version: 1, axis: 2, phase: 1,
            progress: -0.375, velocity_x: -4.5, velocity_y: -4.5 };
        let wire = |gesture| proto::ReliableInputEvent {
            event: Some(proto::reliable_input_event::Event::SystemGesture(gesture)),
        };
        for phase in [1, 2, 4, 8] {
            let event = wire(proto::SystemGesture { phase, ..gesture });
            assert_eq!(proto::ReliableInputEvent::decode(event.encode_to_vec().as_slice()).unwrap(), event);
            let converted = convert_reliable_events(&[event]).unwrap();
            assert!(matches!(converted.as_slice(), [DomainInputEvent::SystemGesture(value)] if value.phase == phase && value.progress == gesture.progress));
        }
        for invalid in [
            proto::SystemGesture { format_version: 2, ..gesture },
            proto::SystemGesture { axis: 3, ..gesture },
            proto::SystemGesture { phase: 3, ..gesture },
            proto::SystemGesture { progress: f64::NAN, ..gesture },
            proto::SystemGesture { velocity_y: 1001.0, ..gesture },
        ] {
            assert!(convert_reliable_events(&[wire(invalid)]).is_err());
        }
        assert!(!proto::InputSessionResult::decode(&[][..]).unwrap().supports_system_gestures);
    }

    #[test]
    fn automation_commands_require_execute_actions_and_validate_identifiers() {
        let features = [proto::Feature::Actions as i32].into_iter().collect();
        let capabilities = [CapabilityId::ActionExecute].into_iter().collect();
        for id in ["automation-1", "", "bad\nidentifier"] {
            for action in [
                proto::command::Action::RunAutomation(proto::RunAutomationCmd{automation_id:id.into()}),
                proto::command::Action::SetAutomationEnabled(proto::SetAutomationEnabledCmd{automation_id:id.into(),enabled:true}),
                proto::command::Action::CancelAutomation(proto::CancelAutomationCmd{activity_id:id.into()}),
            ] {
                let command = proto::Command { action:Some(action) };
                let encoded = command.encode_to_vec();
                assert_eq!(proto::Command::decode(encoded.as_slice()).unwrap(),command);
                assert_eq!(authorize_command(1,&command,&features,&capabilities).is_ok(),id=="automation-1");
                assert!(authorize_command(1,&command,&features,&HashSet::new()).is_err());
                assert!(authorize_command(1,&command,&HashSet::new(),&capabilities).is_err());
            }
        }
    }

    #[test]
    fn command_caches_are_isolated_by_authenticated_key() {
        let first = CommandCacheKey::new(&[1; 32], &[9; 16]);
        let same = CommandCacheKey::new(&[1; 32], &[9; 16]);
        let other = CommandCacheKey::new(&[2; 32], &[9; 16]);

        assert_eq!(first, same);
        assert_ne!(first, other);
    }

    #[tokio::test]
    async fn latest_state_queue_replaces_only_the_same_subscription() {
        let queue = LatestStateQueue::default();
        for sequence in 1..=100 {
            queue.replace(event_frame(
                7,
                sequence,
                sequence as i64,
                proto::event::Data::Gap(proto::EventGap::default()),
            ));
        }
        queue.replace(event_frame(
            9,
            3,
            3,
            proto::event::Data::Gap(proto::EventGap::default()),
        ));

        let first = queue.recv().await;
        let second = queue.recv().await;
        let events: HashMap<_, _> = [first, second]
            .into_iter()
            .filter_map(|frame| match frame.body {
                Some(proto::server_control_frame::Body::Event(event)) => {
                    Some((event.subscription_id, event.sequence))
                }
                _ => None,
            })
            .collect();
        assert_eq!(events.get(&7), Some(&100));
        assert_eq!(events.get(&9), Some(&3));
    }

    #[test]
    fn clipboard_paste_requires_read_write_and_remote_input_capabilities() {
        let command = proto::Command {
            action: Some(proto::command::Action::PasteClipboardRecord(
                proto::PasteClipboardRecordCmd { record_id: 9 },
            )),
        };
        let features = [
            proto::Feature::Clipboard as i32,
            proto::Feature::InputReliable as i32,
        ]
        .into_iter()
        .collect();
        let capabilities = [
            CapabilityId::ClipboardRead,
            CapabilityId::ClipboardWrite,
            CapabilityId::RemoteInputInject,
        ]
        .into_iter()
        .collect();

        assert!(authorize_command(1, &command, &features, &capabilities).is_ok());

        let without_remote_input = [
            CapabilityId::ClipboardRead,
            CapabilityId::ClipboardWrite,
        ]
        .into_iter()
        .collect();
        assert!(authorize_command(1, &command, &features, &without_remote_input).is_err());
    }

    #[test]
    fn clipboard_paste_rejects_zero_record_id() {
        let command = proto::Command {
            action: Some(proto::command::Action::PasteClipboardRecord(
                proto::PasteClipboardRecordCmd { record_id: 0 },
            )),
        };

        assert!(validate_command(&command).is_err());
    }

    #[test]
    fn clipboard_management_commands_require_write_capability() {
        let command = proto::Command {
            action: Some(proto::command::Action::SetClipboardFavorite(
                proto::SetClipboardFavoriteCmd {
                    record_id: 9,
                    favorite: true,
                },
            )),
        };
        let features = [proto::Feature::Clipboard as i32].into_iter().collect();
        let write_capability = [CapabilityId::ClipboardWrite]
            .into_iter()
            .collect();
        let read_capability = [CapabilityId::ClipboardRead]
            .into_iter()
            .collect();

        assert!(authorize_command(1, &command, &features, &write_capability).is_ok());
        assert!(authorize_command(1, &command, &features, &read_capability).is_err());
    }

    #[test]
    fn clipboard_policy_and_cursor_validation_are_bounded() {
        let invalid_policy = proto::Command {
            action: Some(proto::command::Action::UpdateClipboardPolicy(
                proto::UpdateClipboardPolicyCmd {
                    policy: Some(proto::ClipboardPolicy {
                        history_enabled: true,
                        max_items: 0,
                        max_bytes: 100 * 1024 * 1024,
                        retention_days: 30,
                        save_sensitive: false,
                    }),
                },
            )),
        };
        assert!(validate_command(&invalid_policy).is_err());

        let invalid_cursor = proto::Query {
            body: Some(proto::query::Body::GetClipboard(
                proto::GetClipboardRequest {
                    history_limit: 30,
                    cursor_timestamp_ms: Some(1),
                    cursor_id: None,
                    search: String::new(),
                    kinds: Vec::new(),
                    favorite_only: false,
                    label_ids: Vec::new(),
                },
            )),
        };
        assert!(validate_query(&invalid_cursor).is_err());
    }

    #[test]
    fn reliable_scroll_gesture_phases_convert_in_order() {
        let events = [
            proto::ReliableInputEvent {
                event: Some(proto::reliable_input_event::Event::ScrollGesture(
                    proto::ScrollGesture {
                        phase: proto::ScrollGesturePhase::Began as i32,
                    },
                )),
            },
            proto::ReliableInputEvent {
                event: Some(proto::reliable_input_event::Event::ScrollGesture(
                    proto::ScrollGesture {
                        phase: proto::ScrollGesturePhase::Ended as i32,
                    },
                )),
            },
        ];

        assert_eq!(
            convert_reliable_events(&events).unwrap(),
            vec![
                DomainInputEvent::ScrollGesture {
                    phase: DomainScrollGesturePhase::Began,
                },
                DomainInputEvent::ScrollGesture {
                    phase: DomainScrollGesturePhase::Ended,
                },
            ]
        );
    }

    #[test]
    fn reliable_scroll_gesture_rejects_unspecified_phase() {
        let event = proto::ReliableInputEvent {
            event: Some(proto::reliable_input_event::Event::ScrollGesture(
                proto::ScrollGesture {
                    phase: proto::ScrollGesturePhase::Unspecified as i32,
                },
            )),
        };

        assert!(convert_reliable_events(&[event]).is_err());
    }

    #[test]
    fn protocol_v5_rich_text_record_validates_with_html_and_rtf_blobs() {
        let blob = |media_type: &str, marker: u8| proto::BlobRef {
            blob_id: vec![marker; 32],
            size: 128,
            media_type: media_type.into(),
            sha256: vec![marker; 32],
        };
        let record = proto::ClipboardSyncRecord {
            sync_id: "a".repeat(64),
            kind: proto::ClipboardContentKind::Html as i32,
            width: None,
            height: None,
            preview: "Hello".into(),
            source_app: Some("Tests".into()),
            source_device_id: "device-a".into(),
            source_device_name: "Device A".into(),
            captured_at_ms: 1,
            revision: 1,
            updated_by_device_id: "device-a".into(),
            favorite: false,
            deleted: false,
            change_kind: proto::ClipboardSyncChangeKind::Copy as i32,
            live: true,
            favorite_revision: 1,
            favorite_updated_by_device_id: "device-a".into(),
            labels: vec![],
            label_memberships: vec![],
            text_syntax_json: "\"plain\"".into(),
            payload: Some(proto::clipboard_sync_record::Payload::RichText(
                proto::ClipboardRichTextPayload {
                    plain_text: "Hello".into(),
                    html: Some(blob("text/html; charset=utf-8", 1)),
                    rtf: Some(blob("text/rtf", 2)),
                },
            )),
        };

        assert!(validate_clipboard_sync_record(&record).is_ok());
        let mut invalid = record;
        if let Some(proto::clipboard_sync_record::Payload::RichText(rich_text)) =
            invalid.payload.as_mut()
        {
            rich_text.rtf.as_mut().unwrap().media_type = "text/plain".into();
        }
        assert!(validate_clipboard_sync_record(&invalid).is_err());
    }

    struct TestRemoteFileProvider {
        upload_destination: Option<std::path::PathBuf>,
        download_source: Option<std::path::PathBuf>,
        thumbnail: Option<Vec<u8>>,
    }

    #[async_trait::async_trait]
    impl RemoteFileProvider for TestRemoteFileProvider {
        async fn commit_system_upload(&self, _: &str, _: &str, name: &str, temporary: &std::path::Path, expected: &str) -> crate::remote_files::RemoteFileResult<(crate::remote_files::RemoteFileEntry, String)> {
            if expected != "v1" { return Err(crate::remote_files::RemoteFileError::new(RemoteFileErrorCode::Conflict, "stale revision")); }
            let destination = self.upload_destination.as_ref().unwrap();
            tokio::fs::rename(temporary, destination).await.map_err(|e| test_remote_file_error(e.to_string()))?;
            Ok((crate::remote_files::RemoteFileEntry { name: name.into(), relative_path: name.into(), kind: RemoteFileKind::File,
                size: std::fs::metadata(destination).unwrap().len(), modified_at_ms: 1 }, "v2".into()))
        }
        async fn list_shares(&self) -> crate::remote_files::RemoteFileResult<Vec<crate::remote_files::RemoteFileShare>> {
            Ok(vec![crate::remote_files::RemoteFileShare {
                id: "home".into(),
                name: "Home".into(),
                writable: true,
            }])
        }

        async fn list_directory(
            &self,
            _: &str,
            _: &str,
            _: Option<&str>,
            _: u32,
            _: Option<&str>,
            _: crate::remote_files::RemoteFileSortKey,
            _: crate::remote_files::RemoteFileSortDirection,
        ) -> crate::remote_files::RemoteFileResult<crate::remote_files::RemoteFileDirectoryPage> {
            Ok(crate::remote_files::RemoteFileDirectoryPage {
                entries: Vec::new(),
                next_cursor: None,
            })
        }

        async fn create_directory(&self, _: &str, _: &str, _: &str) -> crate::remote_files::RemoteFileResult<crate::remote_files::RemoteFileEntry> {
            Err(test_remote_file_error("unused"))
        }

        async fn rename(&self, _: &str, _: &str, _: &str) -> crate::remote_files::RemoteFileResult<crate::remote_files::RemoteFileEntry> {
            Err(test_remote_file_error("unused"))
        }

        async fn delete(&self, _: &str, _: &str) -> crate::remote_files::RemoteFileResult<()> {
            Err(test_remote_file_error("unused"))
        }

        async fn prepare_download(&self, _: &str, _: &str) -> crate::remote_files::RemoteFileResult<crate::remote_files::RemoteFileDownload> {
            let path = self
                .download_source
                .clone()
                .ok_or_else(|| test_remote_file_error("unused"))?;
            let size = std::fs::metadata(&path)
                .map_err(|error| test_remote_file_error(error.to_string()))?
                .len();
            Ok(crate::remote_files::RemoteFileDownload {
                path,
                entry: crate::remote_files::RemoteFileEntry {
                    name: "download.txt".into(),
                    relative_path: "download.txt".into(),
                    kind: RemoteFileKind::File,
                    size,
                    modified_at_ms: 0,
                },
            })
        }

        async fn prepare_thumbnail(&self, _: &str, _: &str, _: u32) -> crate::remote_files::RemoteFileResult<Option<crate::remote_files::RemoteFileThumbnail>> {
            Ok(self.thumbnail.clone().map(|bytes| crate::remote_files::RemoteFileThumbnail {
                bytes,
                media_type: "image/png".into(),
            }))
        }

        async fn prepare_upload(&self, _: &str, relative_path: &str, name: &str, size: u64, overwrite: bool, _: Option<i64>) -> crate::remote_files::RemoteFileResult<crate::remote_files::RemoteFileUpload> {
            let destination = self
                .upload_destination
                .clone()
                .ok_or_else(|| test_remote_file_error("unused"))?;
            Ok(crate::remote_files::RemoteFileUpload {
                destination,
                overwrite,
                expected_modified_at_ms: None,
                entry: crate::remote_files::RemoteFileEntry {
                    name: name.into(),
                    relative_path: if relative_path.is_empty() {
                        name.into()
                    } else {
                        format!("{relative_path}/{name}")
                    },
                    kind: RemoteFileKind::File,
                    size,
                    modified_at_ms: 0,
                },
            })
        }
    }

    fn test_remote_file_error(message: impl Into<String>) -> crate::remote_files::RemoteFileError {
        crate::remote_files::RemoteFileError::new(
            crate::remote_files::RemoteFileErrorCode::Internal,
            message,
        )
    }

    #[tokio::test]
    async fn remote_file_stream_routes_authenticated_requests() {
        let (client, server) = tokio::io::duplex(4096);
        let (mut client_read, mut client_write) = tokio::io::split(client);
        let (mut server_read, mut server_write) = tokio::io::split(server);
        let handler = tokio::spawn(async move {
            handle_remote_file_stream(
                &mut server_write,
                &mut server_read,
                Some(Arc::new(TestRemoteFileProvider {
                    upload_destination: None,
                    download_source: None,
                    thumbnail: None,
                })),
                RemoteFileAccess::allow_all(),
            )
            .await
        });

        write_remote_message(&mut client_write, &RemoteFileRequest::ListShares)
            .await
            .unwrap();
        let response: RemoteFileResponse = read_remote_message(&mut client_read).await.unwrap();
        assert!(response.ok);
        assert_eq!(response.shares[0].id, "home");
        handler.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn remote_file_stream_writes_uploaded_bytes() {
        let directory = std::env::temp_dir().join(format!(
            "arcrelay-protocol-upload-test-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let destination = directory.join("hello.txt");
        let (client, server) = tokio::io::duplex(4096);
        let (mut client_read, mut client_write) = tokio::io::split(client);
        let (mut server_read, mut server_write) = tokio::io::split(server);
        let provider = TestRemoteFileProvider {
            upload_destination: Some(destination.clone()),
            download_source: None,
            thumbnail: None,
        };
        let handler = tokio::spawn(async move {
            handle_remote_file_stream(
                &mut server_write,
                &mut server_read,
                Some(Arc::new(provider)),
                RemoteFileAccess::allow_all(),
            )
            .await
        });
        let bytes = b"real remote file bytes";
        write_remote_message(
            &mut client_write,
            &RemoteFileRequest::Upload {
                share_id: "home".into(),
                relative_path: "".into(),
                name: "hello.txt".into(),
                size: bytes.len() as u64,
                overwrite: true,
                expected_modified_at_ms: None,
            },
        )
        .await
        .unwrap();
        let ready: RemoteFileResponse = read_remote_message(&mut client_read).await.unwrap();
        assert!(ready.ok);
        client_write.write_all(bytes).await.unwrap();
        client_write.shutdown().await.unwrap();
        let completed: RemoteFileResponse = read_remote_message(&mut client_read).await.unwrap();
        assert!(completed.ok, "{:?}", completed.error);
        handler.await.unwrap().unwrap();
        assert_eq!(std::fs::read(&destination).unwrap(), bytes);
        let _ = std::fs::remove_dir_all(directory);
    }

    async fn run_system_file_upload(
        original: Option<&[u8]>,
        bytes: &[u8],
        declared_size: u64,
        overwrite: bool,
        expected: Option<&str>,
    ) -> (RemoteFileResponse, Vec<u8>) {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("document.txt");
        if let Some(original) = original {
            std::fs::write(&destination, original).unwrap();
        }
        let (client, server) = tokio::io::duplex(4096);
        let (mut reader, mut writer) = tokio::io::split(client);
        let (mut server_reader, mut server_writer) = tokio::io::split(server);
        let provider = TestRemoteFileProvider {
            upload_destination: Some(destination.clone()),
            download_source: None,
            thumbnail: None,
        };
        let handler = tokio::spawn(async move {
            handle_remote_file_stream(
                &mut server_writer, &mut server_reader,
                Some(Arc::new(provider)), RemoteFileAccess::allow_all(),
            ).await
        });
        let request = if let Some(expected) = expected { RemoteFileRequest::ConditionalUpload {
            share_id: "home".into(), relative_path: "".into(), name: "document.txt".into(), size: declared_size, expected_revision: expected.into(),
        } } else { RemoteFileRequest::Upload {
            share_id: "home".into(), relative_path: "".into(),
            name: "document.txt".into(), size: declared_size,
            overwrite, expected_modified_at_ms: None,
        } };
        write_remote_message(&mut writer, &request).await.unwrap();
        let ready: RemoteFileResponse = read_remote_message(&mut reader).await.unwrap();
        assert!(ready.ok, "{:?}", ready.error);
        writer.write_all(bytes).await.unwrap();
        writer.shutdown().await.unwrap();
        let result = read_remote_message(&mut reader).await.unwrap();
        handler.await.unwrap().unwrap();
        let content = std::fs::read(&destination).unwrap();
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
        (result, content)
    }

    #[tokio::test]
    async fn system_file_upload_supports_empty_creation_and_truncation() {
        for original in [None, Some(b"old contents".as_slice())] {
            let (response, content) = run_system_file_upload(original, b"", 0, true, None).await;
            assert!(response.ok, "{:?}", response.error);
            assert!(content.is_empty());
        }
    }

    #[tokio::test]
    async fn conditional_upload_returns_committed_version_and_preserves_original_on_errors() {
        let (response, content) = run_system_file_upload(Some(b"original"), b"", 0, true, Some("v1")).await;
        assert!(response.ok);
        assert_eq!(response.revision, "v2");
        assert!(content.is_empty());
        for (bytes, size, revision, code) in [
            (b"replacement".as_slice(), 11, "stale", RemoteFileErrorCode::Conflict),
            (b"partial".as_slice(), 100, "v1", RemoteFileErrorCode::Unavailable),
            (b"too long".as_slice(), 2, "v1", RemoteFileErrorCode::InvalidArgument),
        ] {
            let (response, content) = run_system_file_upload(Some(b"original"), bytes, size, true, Some(revision)).await;
            assert_eq!(response.error.unwrap().code, code);
            assert_eq!(content, b"original");
        }
    }

    #[tokio::test]
    async fn system_file_operations_require_explicit_peer_permissions() {
        for request in [
            RemoteFileRequest::Stat { share_id: "home".into(), relative_path: "".into() },
            RemoteFileRequest::ReadRange { share_id: "home".into(), relative_path: "a".into(), offset: 0, length: 1, revision: "v1".into() },
            RemoteFileRequest::Move { share_id: "home".into(), relative_path: "a".into(), destination_path: "b".into(), overwrite: false, expected_revision: "v1".into() },
            RemoteFileRequest::ConditionalDelete { share_id: "home".into(), relative_path: "a".into(), expected_revision: "v1".into(), recursive: true },
            RemoteFileRequest::ConditionalUpload { share_id: "home".into(), relative_path: "".into(), name: "a".into(), size: 0, expected_revision: "".into() },
        ] {
            let (client, server) = tokio::io::duplex(4096);
            let (mut reader, mut writer) = tokio::io::split(client);
            let (mut server_reader, mut server_writer) = tokio::io::split(server);
            let handler = tokio::spawn(async move {
                handle_remote_file_stream(&mut server_writer, &mut server_reader,
                    Some(Arc::new(TestRemoteFileProvider { upload_destination: None, download_source: None, thumbnail: None })), RemoteFileAccess::default()).await
            });
            write_remote_message(&mut writer, &request).await.unwrap();
            let response: RemoteFileResponse = read_remote_message(&mut reader).await.unwrap();
            assert_eq!(response.error.unwrap().code, RemoteFileErrorCode::PermissionDenied);
            handler.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn system_file_interrupted_save_preserves_original() {
        let (response, content) = run_system_file_upload(Some(b"original"), b"partial", 100, true, None).await;
        assert!(!response.ok);
        assert_eq!(content, b"original");
    }

    #[tokio::test]
    async fn system_file_create_does_not_replace_existing_destination() {
        let (response, content) = run_system_file_upload(Some(b"original"), b"replacement", 11, false, None).await;
        assert_eq!(response.error.unwrap().code, RemoteFileErrorCode::Conflict);
        assert_eq!(content, b"original");
    }

    #[tokio::test]
    async fn system_file_save_replaces_existing_destination() {
        let (response, content) = run_system_file_upload(Some(b"original"), b"replacement", 11, true, None).await;
        assert!(response.ok, "{:?}", response.error);
        assert_eq!(content, b"replacement");
    }

    #[tokio::test]
    async fn remote_file_stream_reads_downloaded_bytes() {
        let directory = std::env::temp_dir().join(format!(
            "arcrelay-protocol-download-test-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let source = directory.join("download.txt");
        let bytes = b"remote download bytes";
        std::fs::write(&source, bytes).unwrap();
        let (client, server) = tokio::io::duplex(4096);
        let (mut client_read, mut client_write) = tokio::io::split(client);
        let (mut server_read, mut server_write) = tokio::io::split(server);
        let provider = TestRemoteFileProvider {
            upload_destination: None,
            download_source: Some(source),
            thumbnail: None,
        };
        let handler = tokio::spawn(async move {
            handle_remote_file_stream(
                &mut server_write,
                &mut server_read,
                Some(Arc::new(provider)),
                RemoteFileAccess::allow_all(),
            )
            .await
        });
        write_remote_message(
            &mut client_write,
            &RemoteFileRequest::Download {
                share_id: "home".into(),
                relative_path: "download.txt".into(),
            },
        )
        .await
        .unwrap();
        client_write.shutdown().await.unwrap();
        let response: RemoteFileResponse = read_remote_message(&mut client_read).await.unwrap();
        assert!(response.ok);
        let mut received = vec![0_u8; response.entry.unwrap().size as usize];
        client_read.read_exact(&mut received).await.unwrap();
        assert_eq!(received, bytes);
        handler.await.unwrap().unwrap();
        let _ = std::fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn remote_file_stream_transfers_thumbnail_bytes() {
        let bytes = b"small png thumbnail".to_vec();
        let (client, server) = tokio::io::duplex(4096);
        let (mut client_read, mut client_write) = tokio::io::split(client);
        let (mut server_read, mut server_write) = tokio::io::split(server);
        let expected = bytes.clone();
        let handler = tokio::spawn(async move {
            handle_remote_file_stream(
                &mut server_write,
                &mut server_read,
                Some(Arc::new(TestRemoteFileProvider {
                    upload_destination: None,
                    download_source: None,
                    thumbnail: Some(bytes),
                })),
                RemoteFileAccess::allow_all(),
            )
            .await
        });
        write_remote_message(
            &mut client_write,
            &RemoteFileRequest::Thumbnail {
                share_id: "home".into(),
                relative_path: "photo.png".into(),
                max_dimension: 320,
            },
        )
        .await
        .unwrap();
        client_write.shutdown().await.unwrap();
        let response: RemoteFileResponse = read_remote_message(&mut client_read).await.unwrap();
        assert!(response.ok);
        assert_eq!(response.thumbnail_size, expected.len() as u64);
        assert_eq!(response.thumbnail_media_type.as_deref(), Some("image/png"));
        let mut received = vec![0_u8; response.thumbnail_size as usize];
        client_read.read_exact(&mut received).await.unwrap();
        assert_eq!(received, expected);
        handler.await.unwrap().unwrap();
    }
}

#[test]
fn clipboard_sync_id_matches_core_content_addressing() {
    use arcrelay_core::domain::clipboard::ClipboardContentKind;
    use sha2::{Digest, Sha256};
    use xxhash_rust::xxh3::Xxh3;

    let text = Some("ArcRelay".to_string());
    let actual = clipboard_sync_id(ClipboardContentKind::Text, &text, &None, &None, None);
    let mut content = Xxh3::new();
    content.update(b"text\0");
    content.update(b"ArcRelay");
    let content_hash = format!("{:032x}", content.digest128());
    let mut sync = Sha256::new();
    sync.update(b"arcrelay-clipboard-sync-v1\0");
    sync.update(content_hash.as_bytes());
    assert_eq!(actual, format!("{:x}", sync.finalize()));
}
#[test]
fn status_exposes_machine_readable_recovery_semantics() {
    let conflict = status(proto::ErrorCode::Conflict, "state changed");
    assert_eq!(conflict.code, proto::ErrorCode::Conflict as i32);
    assert_eq!(
        conflict.recovery_action,
        proto::RecoveryAction::Refresh as i32
    );
    assert!(!conflict.retryable);

    let unavailable = status(proto::ErrorCode::Unavailable, "try later");
    assert_eq!(
        unavailable.recovery_action,
        proto::RecoveryAction::RetryWithBackoff as i32
    );
    assert!(unavailable.retryable);
}

#[test]
fn internal_status_hides_details_and_has_a_correlation_id() {
    let internal = status(proto::ErrorCode::Internal, "database password leaked");
    assert_eq!(internal.message, "operation failed internally");
    assert!(!internal.error_id.is_empty());
    assert!(!internal.message.contains("password"));
}

#[test]
fn clipboard_metadata_errors_identify_the_field_without_echoing_content() {
    let record = proto::ClipboardSyncRecord {
        sync_id: "a".repeat(64), kind: proto::ClipboardContentKind::Text as i32,
        revision: 1, favorite_revision: 1, captured_at_ms: 1,
        change_kind: proto::ClipboardSyncChangeKind::Copy as i32,
        payload: Some(proto::clipboard_sync_record::Payload::Text("private text".into())),
        ..Default::default()
    };
    assert!(validate_clipboard_sync_record(&record).is_ok());
    let mut invalid = record.clone();
    invalid.favorite_revision = 0;
    assert_eq!(validate_clipboard_sync_record(&invalid), Err("invalid clipboard sync favorite revision"));
    let mut invalid = record.clone();
    invalid.preview = "private preview".repeat(200);
    assert_eq!(validate_clipboard_sync_record(&invalid), Err("clipboard sync preview exceeds limit"));
    let mut invalid = record;
    invalid.labels.push(proto::ClipboardLabel::default());
    assert_eq!(validate_clipboard_sync_record(&invalid), Err("clipboard sync labels are not supported"));
}
