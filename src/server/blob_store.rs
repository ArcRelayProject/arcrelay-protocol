use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::RngCore;
use sha2::{Digest, Sha256};

use crate::message::MAX_BLOB_CHUNK_SIZE;
use crate::proto_msg::proto;

const MAX_CACHED_BLOB_BYTES: usize = 64 * 1024 * 1024;
const MAX_CACHED_BLOB_BYTES_PER_DEVICE: usize = 32 * 1024 * 1024;
const MAX_CACHED_BLOBS: usize = 4096;
const MAX_CACHED_BLOBS_PER_DEVICE: usize = 1024;
const MAX_OUTSTANDING_TICKETS: usize = 4096;
const MAX_OUTSTANDING_TICKETS_PER_DEVICE: usize = 128;
const BLOB_IDLE_TTL: Duration = Duration::from_secs(5 * 60);
const TICKET_TTL: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub(super) enum IssueTicketError {
    NotFound,
    ResourceExhausted,
}

#[derive(Clone)]
pub(super) struct BlobStore {
    inner: Arc<Mutex<BlobStoreInner>>,
}

struct BlobStoreInner {
    entries: HashMap<Vec<u8>, BlobEntry>,
    insertion_order: VecDeque<Vec<u8>>,
    tickets: HashMap<Vec<u8>, TicketEntry>,
    transfer_ids: HashSet<u64>,
    blob_tickets: HashMap<Vec<u8>, usize>,
    device_bytes: HashMap<Vec<u8>, usize>,
    device_entries: HashMap<Vec<u8>, usize>,
    device_tickets: HashMap<Vec<u8>, usize>,
    total_bytes: usize,
    next_prune_at: Option<Instant>,
}

struct BlobEntry {
    reference: proto::BlobRef,
    bytes: Bytes,
    last_access: Instant,
    owners: HashSet<Vec<u8>>,
    delivered_owners: HashSet<Vec<u8>>,
}

struct TicketEntry {
    transfer_id: u64,
    blob_id: Vec<u8>,
    owner: Vec<u8>,
    expires_at: Instant,
}

pub(super) struct RedeemedBlob {
    pub reference: proto::BlobRef,
    pub bytes: Bytes,
}

impl BlobStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BlobStoreInner {
                entries: HashMap::new(),
                insertion_order: VecDeque::new(),
                tickets: HashMap::new(),
                transfer_ids: HashSet::new(),
                blob_tickets: HashMap::new(),
                device_bytes: HashMap::new(),
                device_entries: HashMap::new(),
                device_tickets: HashMap::new(),
                total_bytes: 0,
                next_prune_at: None,
            })),
        }
    }

    pub fn insert(&self, owner: &[u8], bytes: Vec<u8>, media_type: &str) -> Option<proto::BlobRef> {
        if owner.is_empty()
            || owner.len() > 128
            || bytes.is_empty()
            || bytes.len() > MAX_CACHED_BLOB_BYTES
            || media_type.is_empty()
            || media_type.len() > 256
            || media_type.chars().any(char::is_control)
        {
            return None;
        }

        let digest = Sha256::digest(&bytes).to_vec();
        let mut inner = lock(&self.inner);
        inner.prune();
        if inner.entries.contains_key(&digest) {
            let is_existing_owner = inner
                .entries
                .get(&digest)
                .is_some_and(|entry| entry.owners.contains(owner));
            if !is_existing_owner {
                inner.reclaim_delivered_for_owner(owner, bytes.len(), 1);
                let owned_bytes = inner.device_bytes.get(owner).copied().unwrap_or_default();
                let owned_entries = inner.device_entries.get(owner).copied().unwrap_or_default();
                if owned_bytes.saturating_add(bytes.len()) > MAX_CACHED_BLOB_BYTES_PER_DEVICE
                    || owned_entries >= MAX_CACHED_BLOBS_PER_DEVICE
                {
                    return None;
                }
                inner
                    .device_bytes
                    .insert(owner.to_vec(), owned_bytes.saturating_add(bytes.len()));
                inner
                    .device_entries
                    .insert(owner.to_vec(), owned_entries.saturating_add(1));
            }
            let existing = inner.entries.get_mut(&digest).expect("entry was checked");
            existing.last_access = Instant::now();
            if !is_existing_owner {
                existing.owners.insert(owner.to_vec());
            }
            return Some(existing.reference.clone());
        }
        inner.reclaim_delivered_for_owner(owner, bytes.len(), 1);
        let owned_bytes = inner.device_bytes.get(owner).copied().unwrap_or_default();
        let owned_entries = inner.device_entries.get(owner).copied().unwrap_or_default();
        if owned_bytes.saturating_add(bytes.len()) > MAX_CACHED_BLOB_BYTES_PER_DEVICE
            || owned_entries >= MAX_CACHED_BLOBS_PER_DEVICE
        {
            return None;
        }
        if !inner.make_room_for(bytes.len()) {
            return None;
        }

        let reference = proto::BlobRef {
            blob_id: digest.clone(),
            size: bytes.len() as u64,
            media_type: media_type.to_string(),
            sha256: digest.clone(),
        };
        inner.total_bytes = inner.total_bytes.saturating_add(bytes.len());
        let now = Instant::now();
        inner.schedule_prune(now + BLOB_IDLE_TTL);
        inner.insertion_order.push_back(digest.clone());
        inner.entries.insert(
            digest,
            BlobEntry {
                reference: reference.clone(),
                bytes: Bytes::from(bytes),
                last_access: now,
                owners: HashSet::from([owner.to_vec()]),
                delivered_owners: HashSet::new(),
            },
        );
        inner.device_bytes.insert(
            owner.to_vec(),
            owned_bytes.saturating_add(reference.size as usize),
        );
        inner
            .device_entries
            .insert(owner.to_vec(), owned_entries.saturating_add(1));
        Some(reference)
    }

    pub fn issue_ticket(
        &self,
        owner: &[u8],
        blob_id: &[u8],
    ) -> Result<proto::BlobTicket, IssueTicketError> {
        let mut inner = lock(&self.inner);
        inner.prune();
        let reference = {
            let entry = inner
                .entries
                .get_mut(blob_id)
                .ok_or(IssueTicketError::NotFound)?;
            if !entry.owners.contains(owner) {
                return Err(IssueTicketError::NotFound);
            }
            entry.last_access = Instant::now();
            entry.reference.clone()
        };
        if inner.tickets.len() >= MAX_OUTSTANDING_TICKETS
            || inner.device_tickets.get(owner).copied().unwrap_or_default()
                >= MAX_OUTSTANDING_TICKETS_PER_DEVICE
        {
            return Err(IssueTicketError::ResourceExhausted);
        }

        let mut random = rand::rngs::OsRng;
        let transfer_id = loop {
            let value = random.next_u64() & i64::MAX as u64;
            if value != 0 && !inner.transfer_ids.contains(&value) {
                break value;
            }
        };
        let ticket = loop {
            let mut value = vec![0_u8; 32];
            random.fill_bytes(&mut value);
            if !inner.tickets.contains_key(&value) {
                break value;
            }
        };
        let expires_at = Instant::now() + TICKET_TTL;
        inner.schedule_prune(expires_at);
        inner.tickets.insert(
            ticket.clone(),
            TicketEntry {
                transfer_id,
                blob_id: blob_id.to_vec(),
                owner: owner.to_vec(),
                expires_at,
            },
        );
        inner.transfer_ids.insert(transfer_id);
        *inner.blob_tickets.entry(blob_id.to_vec()).or_default() += 1;
        *inner.device_tickets.entry(owner.to_vec()).or_default() += 1;

        Ok(proto::BlobTicket {
            transfer_id,
            ticket,
            blob: Some(reference),
            chunk_size: MAX_BLOB_CHUNK_SIZE as u32,
            expires_at_ms: crate::server::wire::now_ms() + TICKET_TTL.as_millis() as i64,
        })
    }

    pub fn redeem(
        &self,
        owner: &[u8],
        transfer_id: u64,
        ticket: &[u8],
    ) -> Result<RedeemedBlob, &'static str> {
        let mut inner = lock(&self.inner);
        inner.prune();
        let Some(ticket_entry) = inner.tickets.get(ticket) else {
            return Err("blob ticket is invalid or expired");
        };
        if ticket_entry.transfer_id != transfer_id {
            return Err("blob transfer id does not match ticket");
        }
        if ticket_entry.owner.as_slice() != owner {
            return Err("blob ticket belongs to another device");
        }
        if ticket_entry.expires_at <= Instant::now() {
            return Err("blob ticket expired");
        }
        let blob_id = ticket_entry.blob_id.clone();
        inner.remove_ticket(ticket);
        let Some(entry) = inner.entries.get_mut(&blob_id) else {
            return Err("blob was evicted");
        };
        entry.last_access = Instant::now();
        entry.delivered_owners.insert(owner.to_vec());
        Ok(RedeemedBlob {
            reference: entry.reference.clone(),
            bytes: entry.bytes.clone(),
        })
    }
}

impl BlobStoreInner {
    fn schedule_prune(&mut self, deadline: Instant) {
        self.next_prune_at = Some(
            self.next_prune_at
                .map_or(deadline, |next| next.min(deadline)),
        );
    }

    fn owner_has_room(&self, owner: &[u8], bytes: usize, entries: usize) -> bool {
        self.device_bytes
            .get(owner)
            .copied()
            .unwrap_or_default()
            .saturating_add(bytes)
            <= MAX_CACHED_BLOB_BYTES_PER_DEVICE
            && self
                .device_entries
                .get(owner)
                .copied()
                .unwrap_or_default()
                .saturating_add(entries)
                <= MAX_CACHED_BLOBS_PER_DEVICE
    }

    fn reclaim_delivered_for_owner(
        &mut self,
        owner: &[u8],
        additional_bytes: usize,
        additional_entries: usize,
    ) {
        if self.owner_has_room(owner, additional_bytes, additional_entries) {
            return;
        }
        // Only the pressure path walks the insertion order. Move IDs instead
        // of cloning every hash, and remove reclaimed IDs immediately.
        let mut candidates = std::mem::take(&mut self.insertion_order);
        while let Some(blob_id) = candidates.pop_front() {
            if self.owner_has_room(owner, additional_bytes, additional_entries) {
                self.insertion_order.push_back(blob_id);
                break;
            }
            if self.blob_has_ticket(&blob_id) {
                self.insertion_order.push_back(blob_id);
                continue;
            }
            let Some(entry) = self.entries.get_mut(&blob_id) else {
                continue;
            };
            if !entry.delivered_owners.remove(owner) || !entry.owners.remove(owner) {
                self.insertion_order.push_back(blob_id);
                continue;
            }
            let size = entry.bytes.len();
            let remove_entry = entry.owners.is_empty();
            decrement_counter(&mut self.device_bytes, owner, size);
            decrement_counter(&mut self.device_entries, owner, 1);
            if remove_entry {
                self.remove_entry(&blob_id);
            } else {
                self.insertion_order.push_back(blob_id);
            }
        }
        self.insertion_order.extend(candidates);
    }

    fn blob_has_ticket(&self, blob_id: &[u8]) -> bool {
        self.blob_tickets
            .get(blob_id)
            .is_some_and(|count| *count > 0)
    }

    fn prune(&mut self) {
        let now = Instant::now();
        if self.next_prune_at.is_none_or(|deadline| now < deadline) {
            return;
        }
        self.next_prune_at = None;
        let expired_tickets = self
            .tickets
            .iter()
            .filter(|(_, entry)| entry.expires_at <= now)
            .map(|(ticket, _)| ticket.clone())
            .collect::<Vec<_>>();
        for ticket in expired_tickets {
            self.remove_ticket(&ticket);
        }
        self.next_prune_at = self.tickets.values().map(|entry| entry.expires_at).min();

        let insertion_order = std::mem::take(&mut self.insertion_order);
        for blob_id in insertion_order {
            let pinned = self.blob_has_ticket(&blob_id);
            let expired = self.entries.get(&blob_id).is_none_or(|entry| {
                !pinned && now.duration_since(entry.last_access) >= BLOB_IDLE_TTL
            });
            if expired {
                self.remove_entry(&blob_id);
            } else {
                if !pinned {
                    if let Some(entry) = self.entries.get(&blob_id) {
                        self.schedule_prune(entry.last_access + BLOB_IDLE_TTL);
                    }
                }
                self.insertion_order.push_back(blob_id);
            }
        }
    }

    fn remove_entry(&mut self, blob_id: &[u8]) {
        let Some(entry) = self.entries.remove(blob_id) else {
            return;
        };
        self.total_bytes = self.total_bytes.saturating_sub(entry.bytes.len());
        for owner in entry.owners {
            let remove_owner = if let Some(bytes) = self.device_bytes.get_mut(&owner) {
                *bytes = bytes.saturating_sub(entry.bytes.len());
                *bytes == 0
            } else {
                false
            };
            if remove_owner {
                self.device_bytes.remove(&owner);
            }
            let remove_owner = if let Some(count) = self.device_entries.get_mut(&owner) {
                *count = count.saturating_sub(1);
                *count == 0
            } else {
                false
            };
            if remove_owner {
                self.device_entries.remove(&owner);
            }
        }
    }

    fn remove_ticket(&mut self, ticket: &[u8]) {
        let Some(entry) = self.tickets.remove(ticket) else {
            return;
        };
        self.transfer_ids.remove(&entry.transfer_id);
        let remove_blob = if let Some(count) = self.blob_tickets.get_mut(&entry.blob_id) {
            *count = count.saturating_sub(1);
            *count == 0
        } else {
            false
        };
        if remove_blob {
            self.blob_tickets.remove(&entry.blob_id);
        }
        let remove_owner = if let Some(count) = self.device_tickets.get_mut(&entry.owner) {
            *count = count.saturating_sub(1);
            *count == 0
        } else {
            false
        };
        if remove_owner {
            self.device_tickets.remove(&entry.owner);
        }
    }

    fn make_room_for(&mut self, additional_bytes: usize) -> bool {
        if additional_bytes > MAX_CACHED_BLOB_BYTES {
            return false;
        }
        // `prune` has already removed entries whose five-minute reference TTL
        // elapsed. Do not evict a still-live BlobRef merely to create a newer
        // one: a snapshot must not advertise data that disappears before the
        // client has a chance to request its ticket.
        self.entries.len() < MAX_CACHED_BLOBS
            && self.total_bytes.saturating_add(additional_bytes) <= MAX_CACHED_BLOB_BYTES
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

fn decrement_counter(map: &mut HashMap<Vec<u8>, usize>, owner: &[u8], amount: usize) {
    let remove = if let Some(value) = map.get_mut(owner) {
        *value = value.saturating_sub(amount);
        *value == 0
    } else {
        false
    };
    if remove {
        map.remove(owner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduled_pruning_preserves_pinned_blobs_and_removes_expired_tickets() {
        let store = BlobStore::new();
        let owner = [1; 32];
        let blob = store
            .insert(&owner, b"pinned".to_vec(), "image/png")
            .unwrap();
        let ticket = store.issue_ticket(&owner, &blob.blob_id).unwrap();
        let mut inner = lock(&store.inner);
        inner.entries.get_mut(&blob.blob_id).unwrap().last_access =
            Instant::now() - BLOB_IDLE_TTL - Duration::from_secs(1);
        inner.next_prune_at = Some(Instant::now());
        inner.prune();
        assert!(inner.entries.contains_key(&blob.blob_id));
        assert!(inner.next_prune_at.is_some());
        inner.tickets.get_mut(&ticket.ticket).unwrap().expires_at = Instant::now();
        inner.next_prune_at = Some(Instant::now());
        inner.prune();
        assert!(inner.entries.is_empty());
        assert!(inner.tickets.is_empty());
        assert!(inner.device_bytes.is_empty());
        assert!(inner.device_tickets.is_empty());
        assert!(inner.next_prune_at.is_none());
    }

    #[test]
    fn repeated_reclamation_does_not_accumulate_stale_insertion_ids() {
        let store = BlobStore::new();
        let owner = [1; 32];
        for index in 0..MAX_CACHED_BLOBS_PER_DEVICE + 100 {
            let blob = store
                .insert(&owner, index.to_le_bytes().to_vec(), "image/png")
                .unwrap();
            let ticket = store.issue_ticket(&owner, &blob.blob_id).unwrap();
            store
                .redeem(&owner, ticket.transfer_id, &ticket.ticket)
                .unwrap();
        }
        let inner = lock(&store.inner);
        assert_eq!(inner.entries.len(), MAX_CACHED_BLOBS_PER_DEVICE);
        assert_eq!(inner.insertion_order.len(), inner.entries.len());
    }

    #[test]
    #[ignore = "deterministic full-cache ticket latency benchmark"]
    fn performance_full_cache_ticket_round_trip() {
        let store = BlobStore::new();
        let mut target = None;
        for index in 0..MAX_CACHED_BLOBS {
            let owner = (index / MAX_CACHED_BLOBS_PER_DEVICE).to_le_bytes();
            target = Some((
                owner,
                store
                    .insert(&owner, index.to_le_bytes().to_vec(), "image/png")
                    .unwrap(),
            ));
        }
        let (owner, blob) = target.unwrap();
        for forced_scan in [true, false] {
            let mut samples = Vec::new();
            for _ in 0..200 {
                if forced_scan {
                    lock(&store.inner).next_prune_at = Some(Instant::now());
                }
                let started = Instant::now();
                let ticket = store.issue_ticket(&owner, &blob.blob_id).unwrap();
                store
                    .redeem(&owner, ticket.transfer_id, &ticket.ticket)
                    .unwrap();
                samples.push(started.elapsed());
            }
            samples.sort_unstable();
            println!(
                "blob_cache entries={} forced_scan={forced_scan} samples={} p50_us={} p95_us={}",
                MAX_CACHED_BLOBS,
                samples.len(),
                samples[100].as_micros(),
                samples[190].as_micros()
            );
        }
    }

    #[test]
    fn blob_ids_are_content_hashes_and_tickets_are_single_use() {
        let store = BlobStore::new();
        let owner = [1_u8; 32];
        let bytes = b"thumbnail".to_vec();
        let reference = store.insert(&owner, bytes.clone(), "image/png").unwrap();

        assert_eq!(reference.blob_id, Sha256::digest(&bytes).to_vec());
        assert_eq!(reference.sha256, reference.blob_id);
        assert_eq!(reference.size, bytes.len() as u64);

        let ticket = store.issue_ticket(&owner, &reference.blob_id).unwrap();
        let redeemed = store
            .redeem(&owner, ticket.transfer_id, &ticket.ticket)
            .unwrap();
        assert_eq!(redeemed.bytes.as_ref(), bytes.as_slice());
        assert!(store
            .redeem(&owner, ticket.transfer_id, &ticket.ticket)
            .is_err());
    }

    #[test]
    fn empty_blobs_are_not_cached() {
        let store = BlobStore::new();
        assert!(store
            .insert(&[1_u8; 32], vec![], "application/octet-stream")
            .is_none());
    }

    #[test]
    fn delivered_blobs_are_reclaimed_when_owner_cache_is_full() {
        let store = BlobStore::new();
        let owner = [7_u8; 32];
        let first = store
            .insert(
                &owner,
                vec![1; MAX_CACHED_BLOB_BYTES_PER_DEVICE / 2],
                "image/png",
            )
            .unwrap();
        store
            .insert(
                &owner,
                vec![2; MAX_CACHED_BLOB_BYTES_PER_DEVICE / 2],
                "image/png",
            )
            .unwrap();
        assert!(store.insert(&owner, vec![3], "image/png").is_none());

        let first_ticket = store.issue_ticket(&owner, &first.blob_id).unwrap();
        let duplicate_ticket = store.issue_ticket(&owner, &first.blob_id).unwrap();
        store
            .redeem(&owner, first_ticket.transfer_id, &first_ticket.ticket)
            .unwrap();
        assert!(store.insert(&owner, vec![3], "image/png").is_none());
        store
            .redeem(
                &owner,
                duplicate_ticket.transfer_id,
                &duplicate_ticket.ticket,
            )
            .unwrap();

        assert!(store.insert(&owner, vec![3], "image/png").is_some());
    }

    #[test]
    fn global_content_cache_preserves_device_ownership() {
        let store = BlobStore::new();
        let first_owner = [1_u8; 32];
        let second_owner = [2_u8; 32];
        let bytes = b"shared thumbnail".to_vec();
        let reference = store
            .insert(&first_owner, bytes.clone(), "image/png")
            .unwrap();

        assert!(matches!(
            store.issue_ticket(&second_owner, &reference.blob_id),
            Err(IssueTicketError::NotFound)
        ));
        let second_reference = store
            .insert(&second_owner, bytes, "image/png")
            .expect("the content-addressed entry should be shared");
        assert_eq!(reference.blob_id, second_reference.blob_id);

        let ticket = store
            .issue_ticket(&second_owner, &reference.blob_id)
            .unwrap();
        assert!(store
            .redeem(&first_owner, ticket.transfer_id, &ticket.ticket)
            .is_err());
    }

    #[test]
    fn invalid_redeem_does_not_consume_a_valid_ticket() {
        let store = BlobStore::new();
        let owner = [1_u8; 32];
        let reference = store
            .insert(&owner, b"thumbnail".to_vec(), "image/png")
            .unwrap();
        let ticket = store.issue_ticket(&owner, &reference.blob_id).unwrap();

        assert!(store
            .redeem(&owner, ticket.transfer_id + 1, &ticket.ticket)
            .is_err());
        assert!(store
            .redeem(&owner, ticket.transfer_id, &ticket.ticket)
            .is_ok());
    }
}
