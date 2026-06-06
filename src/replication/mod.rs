use std::collections::HashMap;
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VectorClock {
    pub clocks: HashMap<String, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self { clocks: HashMap::new() }
    }

    pub fn increment(&mut self, node_id: &str) {
        *self.clocks.entry(node_id.to_string()).or_insert(0) += 1;
    }

    pub fn merge(&mut self, other: &VectorClock) {
        for (k, &v) in &other.clocks {
            let entry = self.clocks.entry(k.clone()).or_insert(0);
            if v > *entry {
                *entry = v;
            }
        }
    }

    pub fn happens_before(&self, other: &VectorClock) -> bool {
        let mut strictly_less = false;
        for (k, &v) in &self.clocks {
            match other.clocks.get(k) {
                Some(&ov) if v > ov => return false,
                Some(&ov) if v < ov => strictly_less = true,
                None if v > 0 => return false,
                _ => {}
            }
        }
        // Check keys in other that aren't in self
        for (k, &ov) in &other.clocks {
            if !self.clocks.contains_key(k) && ov > 0 {
                strictly_less = true;
            }
        }
        strictly_less
    }

    pub fn concurrent_with(&self, other: &VectorClock) -> bool {
        !self.happens_before(other) && !other.happens_before(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CrdtValue {
    GCounter { value: u64, node_id: String },
    LWWRegister { value: Vec<u8>, timestamp: DateTime<Utc>, node_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationEntry {
    pub term: u64,
    pub index: u64,
    pub command: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub vector_clock: VectorClock,
}

#[derive(Debug, Clone)]
pub enum ReplicationMode {
    PrimaryReplica,
    ActiveActive,
}

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub node_id: String,
    pub addr: String,
    pub port: u16,
    pub last_seen: DateTime<Utc>,
    pub status: PeerStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PeerStatus {
    Connected,
    Disconnected,
    Syncing,
}

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
        Self {
            node_id,
            mode,
            peers: RwLock::new(HashMap::new()),
            log: RwLock::new(Vec::new()),
            crdt_store: RwLock::new(HashMap::new()),
            vector_clock: RwLock::new(VectorClock::new()),
        }
    }

    pub fn add_peer(&self, peer_id: String, addr: String, port: u16) {
        let peer = PeerInfo {
            node_id: peer_id.clone(),
            addr,
            port,
            last_seen: Utc::now(),
            status: PeerStatus::Connected,
        };
        self.peers.write().insert(peer_id, peer);
    }

    pub fn remove_peer(&self, peer_id: &str) {
        self.peers.write().remove(peer_id);
    }

    pub fn append_entry(&self, entry: ReplicationEntry) {
        self.log.write().push(entry);
    }

    pub fn merge_crdt(&self, key: &[u8], incoming: CrdtValue) {
        let mut store = self.crdt_store.write();
        let entry = store.entry(key.to_vec()).or_insert_with(|| match &incoming {
            CrdtValue::GCounter { .. } => CrdtValue::GCounter { value: 0, node_id: self.node_id.clone() },
            CrdtValue::LWWRegister { value, .. } => CrdtValue::LWWRegister { value: value.clone(), timestamp: Utc::now(), node_id: self.node_id.clone() },
        });

        match (&mut *entry, &incoming) {
            (CrdtValue::GCounter { value: v, .. }, CrdtValue::GCounter { value: iv, .. }) => {
                if *iv > *v { *v = *iv; }
            }
            (CrdtValue::LWWRegister { value: rv, timestamp: rt, .. }, CrdtValue::LWWRegister { value: iv, timestamp: it, .. }) => {
                if it > rt || (it == rt && iv > rv) {
                    *rv = iv.clone();
                    *rt = *it;
                }
            }
            _ => {}
        }
    }

    pub fn increment_gcounter(&self, key: &[u8], amount: u64) {
        let mut vc = self.vector_clock.write();
        vc.increment(&self.node_id);

        let mut store = self.crdt_store.write();
        match store.get_mut(key) {
            Some(CrdtValue::GCounter { value, .. }) => {
                *value += amount;
            }
            _ => {
                store.insert(key.to_vec(), CrdtValue::GCounter { value: amount, node_id: self.node_id.clone() });
            }
        }
    }

    pub fn set_lwwregister(&self, key: &[u8], value: Vec<u8>) {
        let mut vc = self.vector_clock.write();
        vc.increment(&self.node_id);

        let mut store = self.crdt_store.write();
        store.insert(key.to_vec(), CrdtValue::LWWRegister {
            value,
            timestamp: Utc::now(),
            node_id: self.node_id.clone(),
        });
    }

    pub fn get_peer_status(&self) -> Vec<PeerInfo> {
        self.peers.read().values().cloned().collect()
    }

    pub fn get_log(&self, from_index: u64) -> Vec<ReplicationEntry> {
        self.log.read().iter()
            .filter(|e| e.index >= from_index)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_clock_ordering() {
        let mut vc1 = VectorClock::new();
        let mut vc2 = VectorClock::new();

        vc1.increment("node1");
        vc1.increment("node1");

        vc2.increment("node1");
        vc2.increment("node1");
        vc2.increment("node2");

        assert!(vc1.happens_before(&vc2));
        assert!(!vc2.happens_before(&vc1));
    }

    #[test]
    fn test_concurrent_vectors() {
        let mut vc1 = VectorClock::new();
        let mut vc2 = VectorClock::new();

        vc1.increment("node1");
        vc2.increment("node2");

        assert!(vc1.concurrent_with(&vc2));
    }

    #[test]
    fn test_gcounter_merge() {
        let engine = ReplicationEngine::new("n1".into(), ReplicationMode::ActiveActive);
        engine.increment_gcounter(b"k1", 5);
        engine.increment_gcounter(b"k1", 3);

        let store = engine.crdt_store.read();
        match store.get(b"k1" as &[u8]).unwrap() {
            CrdtValue::GCounter { value, .. } => assert_eq!(*value, 8),
            _ => panic!("expected GCounter"),
        }
    }

    #[test]
    fn test_lww_register() {
        let engine = ReplicationEngine::new("n1".into(), ReplicationMode::ActiveActive);
        engine.set_lwwregister(b"k1", b"hello".to_vec());

        let store = engine.crdt_store.read();
        match store.get(b"k1" as &[u8]).unwrap() {
            CrdtValue::LWWRegister { value, .. } => assert_eq!(value, b"hello"),
            _ => panic!("expected LWWRegister"),
        }
    }

    #[test]
    fn test_replication_log() {
        let engine = ReplicationEngine::new("n1".into(), ReplicationMode::PrimaryReplica);
        let entry = ReplicationEntry {
            term: 1,
            index: 0,
            command: b"SET k v".to_vec(),
            timestamp: Utc::now(),
            vector_clock: VectorClock::new(),
        };
        engine.append_entry(entry);
        assert_eq!(engine.get_log(0).len(), 1);
        assert_eq!(engine.get_log(1).len(), 0);
    }
}
