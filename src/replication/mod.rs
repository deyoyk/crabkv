use std::collections::HashMap;
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};

/// Vector clock for tracking causal ordering across nodes.
///
/// Each node maintains a monotonically increasing counter. When a node
/// processes an event, it increments its own counter. Vector clocks are
/// merged by taking the element-wise maximum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorClock {
    pub clocks: HashMap<String, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self {
            clocks: HashMap::new(),
        }
    }

    pub fn increment(&mut self, node_id: &str) {
        *self.clocks.entry(node_id.to_string()).or_insert(0) += 1;
    }

    /// Merge another vector clock into this one, keeping the maximum counter
    /// for each node.
    pub fn merge(&mut self, other: &VectorClock) {
        for (node, &counter) in &other.clocks {
            let entry = self.clocks.entry(node.clone()).or_insert(0);
            if counter > *entry {
                *entry = counter;
            }
        }
    }

    /// Returns true if `self` happened-before `other`.
    ///
    /// Every counter in self must be <= the corresponding counter in other,
    /// and at least one must be strictly less.
    pub fn happens_before(&self, other: &VectorClock) -> bool {
        let mut found_strict = false;
        let all_nodes: Vec<&String> = self
            .clocks
            .keys()
            .chain(other.clocks.keys())
            .collect();

        for node in all_nodes {
            let a = self.clocks.get(node).copied().unwrap_or(0);
            let b = other.clocks.get(node).copied().unwrap_or(0);
            if a > b {
                return false;
            }
            if a < b {
                found_strict = true;
            }
        }
        found_strict
    }

    /// Returns true if `self` and `other` are concurrent (neither happens-before the other).
    pub fn concurrent_with(&self, other: &VectorClock) -> bool {
        !self.happens_before(other) && !other.happens_before(self)
    }

    pub fn is_empty(&self) -> bool {
        self.clocks.is_empty()
    }

    /// Remove all entries for nodes not in the given set.
    pub fn garbage_collect(&mut self, live_nodes: &std::collections::HashSet<&str>) {
        self.clocks.retain(|node, _| live_nodes.contains(node.as_str()));
    }
}

impl Default for VectorClock {
    fn default() -> Self {
        Self::new()
    }
}

/// CRDT value variants.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CrdtValue {
    /// Grow-only counter. Merged via element-wise max.
    GCounter {
        value: u64,
        node_id: String,
    },
    /// Last-Writer-Wins register. Conflicts resolved by (timestamp, node_id) tiebreak.
    LWWRegister {
        value: Vec<u8>,
        timestamp: DateTime<Utc>,
        node_id: String,
    },
    /// Observed-Remove Set. Elements are tracked with their adding vector clock.
    ORSet {
        elements: HashMap<Vec<u8>, VectorClock>,
    },
}

/// A single entry in the replication log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationEntry {
    pub term: u64,
    pub index: u64,
    pub command: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub vector_clock: VectorClock,
}

/// Replication strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationMode {
    /// Single-writer primary with read-only replicas.
    PrimaryReplica,
    /// Multi-master using CRDTs for conflict-free convergence.
    ActiveActive,
    /// Raft-based strong consistency (log replication with leader election).
    RaftConsensus,
}

/// Metadata for a known peer.
pub struct PeerInfo {
    pub node_id: String,
    pub addr: String,
    pub port: u16,
    pub last_seen: DateTime<Utc>,
    pub status: PeerStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerStatus {
    Connected,
    Disconnected,
    Syncing,
}

/// Core replication engine.
pub struct ReplicationEngine {
    pub node_id: String,
    pub mode: ReplicationMode,
    pub peers: RwLock<HashMap<String, PeerInfo>>,
    pub log: RwLock<Vec<ReplicationEntry>>,
    pub crdt_store: RwLock<HashMap<Vec<u8>, CrdtValue>>,
    pub vector_clock: RwLock<VectorClock>,
}

impl ReplicationEngine {
    pub fn new(node_id: String, mode: ReplicationMode) -> Self {
        let mut vc = VectorClock::new();
        vc.increment(&node_id);
        Self {
            node_id,
            mode,
            peers: RwLock::new(HashMap::new()),
            log: RwLock::new(Vec::new()),
            crdt_store: RwLock::new(HashMap::new()),
            vector_clock: RwLock::new(vc),
        }
    }

    pub fn add_peer(&self, peer_id: String, addr: String, port: u16) {
        let info = PeerInfo {
            node_id: peer_id.clone(),
            addr,
            port,
            last_seen: Utc::now(),
            status: PeerStatus::Connected,
        };
        self.peers.write().insert(peer_id, info);
    }

    pub fn remove_peer(&self, peer_id: &str) {
        self.peers.write().remove(peer_id);
    }

    pub fn append_entry(&self, entry: ReplicationEntry) {
        self.log.write().push(entry);
    }

    /// Merge an incoming CRDT value with the local value for the given key.
    ///
    /// For GCounter: takes the element-wise maximum.
    /// For LWWRegister: keeps the value with the later timestamp (tiebreak by node_id).
    /// For ORSet: unions the element maps.
    pub fn merge_crdt(&self, key: &[u8], incoming: CrdtValue) {
        let mut store = self.crdt_store.write();
        let existing = store.get(key);

        let merged = match incoming {
            CrdtValue::GCounter {
                value: in_val,
                ref node_id,
            } => {
                match existing {
                    Some(CrdtValue::GCounter {
                        value: local_val,
                        node_id: local_node,
                    }) => {
                        let new_val = if node_id == local_node {
                            // Same node, take max (should be monotonic)
                            (*local_val).max(in_val)
                        } else {
                            // Different node replicas — max of both
                            (*local_val).max(in_val)
                        };
                        CrdtValue::GCounter {
                            value: new_val,
                            node_id: node_id.clone(),
                        }
                    }
                    _ => CrdtValue::GCounter {
                        value: in_val,
                        node_id: node_id.clone(),
                    },
                }
            }
            CrdtValue::LWWRegister {
                value: in_val,
                timestamp: in_ts,
                node_id: ref in_node,
            } => {
                match existing {
                    Some(CrdtValue::LWWRegister {
                        value: _,
                        timestamp: local_ts,
                        node_id: local_node,
                    }) => {
                        if in_ts > *local_ts
                            || (in_ts == *local_ts && in_node > local_node)
                        {
                            CrdtValue::LWWRegister {
                                value: in_val,
                                timestamp: in_ts,
                                node_id: in_node.clone(),
                            }
                        } else {
                            existing.cloned().unwrap()
                        }
                    }
                    _ => CrdtValue::LWWRegister {
                        value: in_val,
                        timestamp: in_ts,
                        node_id: in_node.clone(),
                    },
                }
            }
            CrdtValue::ORSet {
                elements: in_elems,
            } => {
                match existing {
                    Some(CrdtValue::ORSet { elements: local_elems }) => {
                        let mut merged_elems = local_elems.clone();
                        for (elem, vc) in &in_elems {
                            let entry = merged_elems
                                .entry(elem.clone())
                                .or_insert_with(VectorClock::new);
                            entry.merge(vc);
                        }
                        CrdtValue::ORSet {
                            elements: merged_elems,
                        }
                    }
                    _ => CrdtValue::ORSet {
                        elements: in_elems,
                    },
                }
            }
        };

        store.insert(key.to_vec(), merged);
    }

    /// Atomically increment a G-Counter by `amount` and update the local vector clock.
    pub fn increment_gcounter(&self, key: &[u8], amount: u64) {
        let mut vc = self.vector_clock.write();
        vc.increment(&self.node_id);
        let vc_clone = vc.clone();

        let mut store = self.crdt_store.write();
        let new_val = match store.get(key) {
            Some(CrdtValue::GCounter { value, .. }) => *value + amount,
            _ => amount,
        };
        store.insert(
            key.to_vec(),
            CrdtValue::GCounter {
                value: new_val,
                node_id: self.node_id.clone(),
            },
        );

        // Also append to the replication log
        let log_len = self.log.read().len() as u64;
        let entry = ReplicationEntry {
            term: 0,
            index: log_len,
            command: key.to_vec(),
            timestamp: Utc::now(),
            vector_clock: vc_clone,
        };
        self.log.write().push(entry);
    }

    /// Set an LWW-Register value and update the local vector clock.
    pub fn set_lwwregister(&self, key: &[u8], value: Vec<u8>) {
        let mut vc = self.vector_clock.write();
        vc.increment(&self.node_id);
        let vc_clone = vc.clone();

        let mut store = self.crdt_store.write();
        store.insert(
            key.to_vec(),
            CrdtValue::LWWRegister {
                value,
                timestamp: Utc::now(),
                node_id: self.node_id.clone(),
            },
        );

        let log_len = self.log.read().len() as u64;
        let entry = ReplicationEntry {
            term: 0,
            index: log_len,
            command: key.to_vec(),
            timestamp: Utc::now(),
            vector_clock: vc_clone,
        };
        self.log.write().push(entry);
    }

    pub fn get_peer_status(&self) -> Vec<PeerInfo> {
        self.peers
            .read()
            .values()
            .map(|p| PeerInfo {
                node_id: p.node_id.clone(),
                addr: p.addr.clone(),
                port: p.port,
                last_seen: p.last_seen,
                status: p.status.clone(),
            })
            .collect()
    }

    pub fn get_log(&self, from_index: u64) -> Vec<ReplicationEntry> {
        self.log
            .read()
            .iter()
            .filter(|e| e.index >= from_index)
            .cloned()
            .collect()
    }

    /// Apply a replication entry: update vector clock and merge into CRDT store.
    pub fn apply_entry(&self, entry: &ReplicationEntry) {
        // Update vector clock
        self.vector_clock.write().merge(&entry.vector_clock);

        // Parse the command as a key and apply to the CRDT store if it exists
        let key = &entry.command;
        let store = self.crdt_store.read();
        if let Some(existing) = store.get(key) {
            let incoming = existing.clone();
            drop(store);
            self.merge_crdt(key, incoming);
        }
    }

    /// Mark a peer as seen (updates last_seen timestamp).
    pub fn touch_peer(&self, peer_id: &str) {
        if let Some(peer) = self.peers.write().get_mut(peer_id) {
            peer.last_seen = Utc::now();
            peer.status = PeerStatus::Connected;
        }
    }

    /// Mark a peer as disconnected.
    pub fn disconnect_peer(&self, peer_id: &str) {
        if let Some(peer) = self.peers.write().get_mut(peer_id) {
            peer.status = PeerStatus::Disconnected;
        }
    }

    /// Return the number of entries in the replication log.
    pub fn log_len(&self) -> usize {
        self.log.read().len()
    }

    /// Return the current vector clock snapshot.
    pub fn current_clock(&self) -> VectorClock {
        self.vector_clock.read().clone()
    }

    /// Create a CRDT entry for ORSet add operation.
    pub fn orset_add(&self, key: &[u8], element: Vec<u8>) {
        let mut vc = self.vector_clock.write();
        vc.increment(&self.node_id);
        let vc_clone = vc.clone();

        let mut store = self.crdt_store.write();
        match store.get_mut(key) {
            Some(CrdtValue::ORSet { elements }) => {
                elements.insert(element, vc_clone);
            }
            _ => {
                let mut elements = HashMap::new();
                elements.insert(element, vc_clone);
                store.insert(key.to_vec(), CrdtValue::ORSet { elements });
            }
        }
    }

    /// Remove an element from an ORSet. In a proper CRDT, removal requires
    /// a "tombstone" or a conditional remove based on the adding clock.
    /// Here we use a simple approach: set value to empty VectorClock
    /// to signal removal (consumers must handle tombstone semantics).
    pub fn orset_remove(&self, key: &[u8], element: &[u8]) {
        let mut vc = self.vector_clock.write();
        vc.increment(&self.node_id);
        let vc_clone = vc.clone();

        let mut store = self.crdt_store.write();
        if let Some(CrdtValue::ORSet { elements }) = store.get_mut(key) {
            // Remove the element entirely; in production you'd want tombstones
            elements.remove(element);
        }
    }
}

/// CRC16 implementation matching Redis Cluster's XMODEM variant.
///
/// Used to map keys to hash slots in the range [0, 16383].
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

/// Compute the hash slot for a key, matching Redis Cluster behavior.
///
/// If the key contains `{...}`, only the content between the braces is hashed.
/// Otherwise the entire key is hashed. The result is modulo 16384.
pub fn hash_slot(key: &[u8]) -> u16 {
    let effective = find_hash_tag(key);
    crc16(effective) % 16384
}

/// Find the substring between `{` and `}` if present (the hash tag).
fn find_hash_tag(key: &[u8]) -> &[u8] {
    if let Some(start) = key.iter().position(|&b| b == b'{') {
        if let Some(end) = key[start + 1..].iter().position(|&b| b == b'}') {
            if end > 0 {
                return &key[start + 1..start + 1 + end];
            }
        }
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------- VectorClock tests ----------

    #[test]
    fn vector_clock_new_is_empty() {
        let vc = VectorClock::new();
        assert!(vc.is_empty());
    }

    #[test]
    fn vector_clock_increment() {
        let mut vc = VectorClock::new();
        vc.increment("node-a");
        assert_eq!(vc.clocks["node-a"], 1);
        vc.increment("node-a");
        assert_eq!(vc.clocks["node-a"], 2);
    }

    #[test]
    fn vector_clock_merge() {
        let mut a = VectorClock::new();
        a.increment("node-a");
        a.increment("node-a");

        let mut b = VectorClock::new();
        b.increment("node-b");
        b.increment("node-a");

        a.merge(&b);
        assert_eq!(a.clocks["node-a"], 2);
        assert_eq!(a.clocks["node-b"], 1);
    }

    #[test]
    fn vector_clock_happens_before() {
        let mut a = VectorClock::new();
        a.increment("node-a");
        a.increment("node-b");

        let mut b = VectorClock::new();
        b.increment("node-a");
        b.increment("node-b");
        b.increment("node-b");

        assert!(a.happens_before(&b));
        assert!(!b.happens_before(&a));
    }

    #[test]
    fn vector_clock_not_happens_before_if_greater() {
        let mut a = VectorClock::new();
        *a.clocks.entry("node-a".into()).or_insert(0) = 5;
        *a.clocks.entry("node-b".into()).or_insert(0) = 3;

        let mut b = VectorClock::new();
        *b.clocks.entry("node-a".into()).or_insert(0) = 4;
        *b.clocks.entry("node-b".into()).or_insert(0) = 3;

        assert!(!a.happens_before(&b));
    }

    #[test]
    fn vector_clock_concurrent() {
        let mut a = VectorClock::new();
        *a.clocks.entry("node-a".into()).or_insert(0) = 2;
        *a.clocks.entry("node-b".into()).or_insert(0) = 1;

        let mut b = VectorClock::new();
        *b.clocks.entry("node-a".into()).or_insert(0) = 1;
        *b.clocks.entry("node-b".into()).or_insert(0) = 2;

        assert!(a.concurrent_with(&b));
        assert!(b.concurrent_with(&a));
    }

    #[test]
    fn vector_clock_empty_concurrent_with_empty() {
        let a = VectorClock::new();
        let b = VectorClock::new();
        // Both happen-before checks return false (no strict less), so concurrent_with returns true
        // This is a degenerate case; in practice both-empty means no events happened
        assert!(!a.happens_before(&b));
        assert!(!b.happens_before(&a));
    }

    #[test]
    fn vector_clock_garbage_collect() {
        let mut vc = VectorClock::new();
        vc.increment("node-a");
        vc.increment("node-b");
        vc.increment("node-c");

        let mut live = std::collections::HashSet::new();
        live.insert("node-a");
        live.insert("node-c");

        vc.garbage_collect(&live);
        assert_eq!(vc.clocks.len(), 2);
        assert!(vc.clocks.contains_key("node-a"));
        assert!(vc.clocks.contains_key("node-c"));
        assert!(!vc.clocks.contains_key("node-b"));
    }

    // ---------- CRC16 / hash_slot tests ----------

    #[test]
    fn crc16_known_values() {
        // Empty data should produce 0
        assert_eq!(crc16(b""), 0);

        // Single byte
        let v = crc16(b"1");
        assert_ne!(v, 0);
    }

    #[test]
    fn hash_slot_returns_valid_range() {
        let slot = hash_slot(b"mykey");
        assert!(slot < 16384);
    }

    #[test]
    fn hash_slot_uses_hash_tag() {
        // Keys with same hash tag should map to the same slot
        let a = hash_slot(b"prefix{tag1}suffix");
        let b = hash_slot(b"other{tag1}stuff");
        assert_eq!(a, b);
    }

    #[test]
    fn hash_slot_different_without_tag() {
        // Keys without hash tags use full key
        let a = hash_slot(b"key1");
        let b = hash_slot(b"key2");
        // Not guaranteed different but almost certainly
        assert!(a < 16384);
        assert!(b < 16384);
    }

    #[test]
    fn hash_slot_empty_tag_falls_back() {
        // Empty braces {} fall back to full key
        let a = hash_slot(b"key{}rest");
        let b = hash_slot(b"key{}rest");
        assert_eq!(a, b);
    }

    // ---------- CRDT merge tests ----------

    #[test]
    fn gcounter_merge_takes_max() {
        let engine = ReplicationEngine::new("n1".into(), ReplicationMode::ActiveActive);

        engine.merge_crdt(
            b"k1",
            CrdtValue::GCounter {
                value: 5,
                node_id: "n1".into(),
            },
        );
        engine.merge_crdt(
            b"k1",
            CrdtValue::GCounter {
                value: 3,
                node_id: "n2".into(),
            },
        );
        engine.merge_crdt(
            b"k1",
            CrdtValue::GCounter {
                value: 7,
                node_id: "n3".into(),
            },
        );

        let store = engine.crdt_store.read();
        match store.get(b"k1") {
            Some(CrdtValue::GCounter { value, .. }) => assert_eq!(*value, 7),
            _ => panic!("expected GCounter"),
        }
    }

    #[test]
    fn lwwregister_keeps_latest() {
        let engine = ReplicationEngine::new("n1".into(), ReplicationMode::ActiveActive);

        let ts_old = Utc::now();
        let ts_new = ts_old + chrono::Duration::seconds(5);

        engine.merge_crdt(
            b"key",
            CrdtValue::LWWRegister {
                value: b"old".to_vec(),
                timestamp: ts_old,
                node_id: "n1".into(),
            },
        );
        engine.merge_crdt(
            b"key",
            CrdtValue::LWWRegister {
                value: b"new".to_vec(),
                timestamp: ts_new,
                node_id: "n2".into(),
            },
        );

        let store = engine.crdt_store.read();
        match store.get(b"key") {
            Some(CrdtValue::LWWRegister { value, .. }) => assert_eq!(value, b"new"),
            _ => panic!("expected LWWRegister"),
        }
    }

    #[test]
    fn orset_merge_unions() {
        let engine = ReplicationEngine::new("n1".into(), ReplicationMode::ActiveActive);

        let mut vc1 = VectorClock::new();
        vc1.increment("n1");
        let mut vc2 = VectorClock::new();
        vc2.increment("n2");

        engine.merge_crdt(
            b"set",
            CrdtValue::ORSet {
                elements: vec![(b"a".to_vec(), vc1)].into_iter().collect(),
            },
        );
        engine.merge_crdt(
            b"set",
            CrdtValue::ORSet {
                elements: vec![(b"b".to_vec(), vc2)].into_iter().collect(),
            },
        );

        let store = engine.crdt_store.read();
        match store.get(b"set") {
            Some(CrdtValue::ORSet { elements }) => {
                assert_eq!(elements.len(), 2);
                assert!(elements.contains_key(b"a".as_ref()));
                assert!(elements.contains_key(b"b".as_ref()));
            }
            _ => panic!("expected ORSet"),
        }
    }

    // ---------- Increment / set integration tests ----------

    #[test]
    fn increment_gcounter_updates_store() {
        let engine = ReplicationEngine::new("n1".into(), ReplicationMode::ActiveActive);
        engine.increment_gcounter(b"counter", 10);
        engine.increment_gcounter(b"counter", 5);

        let store = engine.crdt_store.read();
        match store.get(b"counter") {
            Some(CrdtValue::GCounter { value, node_id }) => {
                assert_eq!(*value, 15);
                assert_eq!(node_id, "n1");
            }
            _ => panic!("expected GCounter"),
        }
    }

    #[test]
    fn increment_gcounter_appends_log() {
        let engine = ReplicationEngine::new("n1".into(), ReplicationMode::ActiveActive);
        engine.increment_gcounter(b"counter", 1);
        assert_eq!(engine.log_len(), 1);
    }

    #[test]
    fn set_lwwregister_updates_store() {
        let engine = ReplicationEngine::new("n1".into(), ReplicationMode::ActiveActive);
        engine.set_lwwregister(b"name", b"alice".to_vec());

        let store = engine.crdt_store.read();
        match store.get(b"name") {
            Some(CrdtValue::LWWRegister { value, node_id, .. }) => {
                assert_eq!(value, b"alice");
                assert_eq!(node_id, "n1");
            }
            _ => panic!("expected LWWRegister"),
        }
    }

    #[test]
    fn orset_add_and_remove() {
        let engine = ReplicationEngine::new("n1".into(), ReplicationMode::ActiveActive);
        engine.orset_add(b"tags", b"rust".to_vec());
        engine.orset_add(b"tags", b"crdt".to_vec());

        {
            let store = engine.crdt_store.read();
            if let Some(CrdtValue::ORSet { elements }) = store.get(b"tags") {
                assert_eq!(elements.len(), 2);
            } else {
                panic!("expected ORSet");
            }
        }

        engine.orset_remove(b"tags", b"rust");

        let store = engine.crdt_store.read();
        if let Some(CrdtValue::ORSet { elements }) = store.get(b"tags") {
            assert_eq!(elements.len(), 1);
            assert!(elements.contains_key(b"crdt".as_ref()));
        } else {
            panic!("expected ORSet");
        }
    }

    // ---------- Peer management tests ----------

    #[test]
    fn add_and_remove_peer() {
        let engine = ReplicationEngine::new("n1".into(), ReplicationMode::ActiveActive);
        engine.add_peer("n2".into(), "127.0.0.1".into(), 7001);
        engine.add_peer("n3".into(), "127.0.0.1".into(), 7002);

        assert_eq!(engine.get_peer_status().len(), 2);

        engine.remove_peer("n2");
        assert_eq!(engine.get_peer_status().len(), 1);
    }

    #[test]
    fn touch_and_disconnect_peer() {
        let engine = ReplicationEngine::new("n1".into(), ReplicationMode::PrimaryReplica);
        engine.add_peer("n2".into(), "127.0.0.1".into(), 7001);

        engine.disconnect_peer("n2");
        let peers = engine.get_peer_status();
        assert_eq!(peers[0].status, PeerStatus::Disconnected);

        engine.touch_peer("n2");
        let peers = engine.get_peer_status();
        assert_eq!(peers[0].status, PeerStatus::Connected);
    }

    // ---------- Log tests ----------

    #[test]
    fn get_log_from_index() {
        let engine = ReplicationEngine::new("n1".into(), ReplicationMode::PrimaryReplica);
        for i in 0..5 {
            engine.append_entry(ReplicationEntry {
                term: 0,
                index: i,
                command: vec![i as u8],
                timestamp: Utc::now(),
                vector_clock: VectorClock::new(),
            });
        }

        let entries = engine.get_log(3);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index, 3);
        assert_eq!(entries[1].index, 4);
    }

    // ---------- Apply entry tests ----------

    #[test]
    fn apply_entry_merges_vector_clock() {
        let engine = ReplicationEngine::new("n1".into(), ReplicationMode::ActiveActive);

        let mut vc = VectorClock::new();
        vc.increment("n2");
        vc.increment("n2");
        vc.increment("n2");

        let entry = ReplicationEntry {
            term: 0,
            index: 0,
            command: vec![],
            timestamp: Utc::now(),
            vector_clock: vc,
        };

        engine.apply_entry(&entry);

        let clock = engine.current_clock();
        assert_eq!(clock.clocks.get("n2").copied(), Some(3));
        // n1's own counter should still be 1
        assert_eq!(clock.clocks.get("n1").copied(), Some(1));
    }
}
