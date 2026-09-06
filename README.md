# ArcRelay v1 protocol

ArcRelay has one device-level LAN protocol. `arcrelay-network` owns the only
QUIC endpoint, `_arcrelay._udp.local.` discovery service, Ed25519 root identity,
pairing flow, peer repository, and capability authorization.

The workspace separates that contract into three layers:

- `arcrelay-wire`: protobuf types, ALPN `arcrelay/1`, stream identifiers, and
  size limits.
- `arcrelay-transport`: bounded length-prefixed frame I/O.
- `arcrelay-protocol`: the remote-control feature schema and desktop-domain
  adapters. It neither creates endpoints nor persists trust.

There is no compatibility path for the former feature-specific endpoints,
certificates, discovery services, or trust stores.

## Identity, discovery, and pairing

`arcrelay-network::NetworkRuntime` derives a stable `DeviceId` from the root
Ed25519 public key and binds the ephemeral TLS endpoint certificate to that key.
mDNS is only an address hint; the v1 handshake verifies the advertised identity
and TLS exporter-bound signatures before accepting a session.

Pairing uses a short authentication string shown on both devices. Approval may
select a strict subset of the requested directional capability grants; only
that subset is atomically stored with the peer record. Clipboard, remote files,
cross-screen input, nearby transfer, and printing therefore reuse the same
paired device. A feature cannot establish or persist private trust.

Forgetting a device removes its peer record and grants centrally. Feature-local
preferences such as automatic transfer receipt are policy only and never count
as cryptographic trust.

## Sessions

Every authenticated connection declares one `SessionKind` from
`arcrelay-wire/proto/common.proto`:

| Session | Purpose |
| --- | --- |
| `Pairing` | SAS verification and atomic grant installation |
| `Control` | system, process, media, clipboard, windows, actions, notifications, remote files |
| `RealtimeInput` | latency-sensitive cross-screen keyboard and pointer data |
| `FileTransfer` | nearby file-transfer control and payloads |
| `Print` | printer discovery, job control, and document upload |

These sessions share one endpoint and root trust while retaining independent
QUIC congestion and lifecycle boundaries. Concurrent duplicate sessions of the
same kind converge deterministically.

The first bounded frame on a feature stream is `FeaturePayload`. It declares a
stable feature identifier, independent major/minor range, non-zero stream ID,
optional operation name, and bounded opening payload. The receiver authorizes
the session kind and required directional capability before handing it to
business code.

## Control feature

`arcrelay-wire/proto/arcrelay.proto` defines the control feature messages.
Authentication and pairing do not appear in this schema because they have
already completed in the v1 session envelope. The first control frame is a
`ControlHello`; the server returns a `ControlWelcome` containing the negotiated
features, receive limits, current grants, authorization epoch, and server time.

- Requests use non-zero correlation IDs and command idempotency keys.
- Queries, commands, and subscriptions require both the declared feature and
  the mapped root capability grant.
- State subscriptions use monotonic sequence numbers and bounded replacement
  queues; non-replaceable action output reports explicit gaps.
- Heartbeats, request deadlines, collection limits, and frame limits are
  enforced by both peers.
- Large blobs use content-addressed tickets and separate bounded streams.
- Reliable remote-control input uses an ordered auxiliary stream and an
  OS-permission-gated input lease.
- Remote-file operations use versionable protobuf messages and one auxiliary
  stream per operation so file bytes cannot block the control plane. Directory
  listings are always bounded and cursor-paginated.

Printing and file transfer are authorized v1 sessions, not public or
independently paired services. Print requests carry correlation IDs and
deadlines; document uploads require a short-lived, one-time ticket. Transfer
payload identity comes only from the authenticated session, never from
self-asserted offer fields.

## Stream kinds inside a feature connection

| Kind | Purpose |
| --- | --- |
| 1 | Arc Input control messages |
| 2 | ordered remote-control input |
| 3 | blob download |
| 4 | file payload |
| 5 | transfer control |
| 6 | clipboard blob upload |
| 7 | remote-file operation |
| 8 | print document upload |

Every framed protobuf message uses a four-byte big-endian length prefix. Raw
payload streams declare and validate their exact byte length and digest. Setup,
idle, and total deadlines bound resource occupancy.

## Code generation

The canonical schemas and Rust code generation are owned by `arcrelay-wire`.
This crate consumes those generated types and contains no platform-client code
generation workflow. Compatibility rules and the `buf` lint configuration live
in that repository; the private mobile repository verifies its checked-in Dart
output against the same schema in CI.
