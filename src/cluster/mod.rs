use std::collections::HashMap;
use parking_lot::RwLock;

/// Total number of hash slots in the cluster (matches Redis Cluster).
pub const HASH_SLOTS: u16 = 16384;

/// Metadata for a cluster member.
#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub node_id: String,
    pub addr: String,
    pub port: u16,
    pub slots: Vec<u16>,
    pub is_master: bool,
    pub replicas: Vec<String>,
}

/// A contiguous or single slot assignment.
#[derive(Debug, Clone)]
pub struct SlotRange {
    pub start: u16,
    pub end: u16,
    pub node_id: String,
}

/// Result of routing a key to a cluster node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteResult {
    /// The key maps to the given node.
    Ok(String),
    /// The key should be served by a different node (MOVED redirection).
    Moved(u16, String),
    /// Temporary redirection during slot migration (ASK).
    Ask(u16, String),
}

/// Cluster-wide state: node registry, slot ownership, and routing.
pub struct ClusterState {
    pub nodes: RwLock<HashMap<String, ClusterNode>>,
    pub slot_map: RwLock<Vec<Option<String>>>,
    pub myself: String,
}

impl ClusterState {
    pub fn new(node_id: String) -> Self {
        let mut slot_map = Vec::with_capacity(HASH_SLOTS as usize);
        slot_map.resize_with(HASH_SLOTS as usize, || None);
        Self {
            nodes: RwLock::new(HashMap::new()),
            slot_map: RwLock::new(slot_map),
            myself: node_id,
        }
    }

    pub fn add_node(&self, node: ClusterNode) {
        let node_id = node.node_id.clone();
        self.nodes.write().insert(node_id, node.clone());
        for slot in &node.slots {
            *self.slot_map.write().get_mut(*slot as usize).unwrap() =
                Some(node.node_id.clone());
        }
    }

    pub fn remove_node(&self, node_id: &str) {
        let node = match self.nodes.write().remove(node_id) {
            Some(n) => n,
            None => return,
        };
        for slot in &node.slots {
            let mut sm = self.slot_map.write();
            if sm[*slot as usize].as_deref() == Some(node_id) {
                sm[*slot as usize] = None;
            }
        }
    }

    pub fn assign_slot(&self, slot: u16, node_id: &str) {
        assert!(
            (slot as usize) < HASH_SLOTS as usize,
            "slot {slot} out of range"
        );
        *self.slot_map.write().get_mut(slot as usize).unwrap() =
            Some(node_id.to_string());

        // Also update the node's slot list
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.get_mut(node_id) {
            if !node.slots.contains(&slot) {
                node.slots.push(slot);
                node.slots.sort_unstable();
            }
        }
    }

    /// Route a key to the node that owns its hash slot.
    ///
    /// Returns `RouteResult::Ok` if the local node owns the slot,
    /// `RouteResult::Moved` otherwise, so the caller can redirect the client.
    pub fn route_key(&self, key: &[u8]) -> RouteResult {
        let slot = hash_slot(key);
        let sm = self.slot_map.read();
        match sm.get(slot as usize) {
            Some(Some(node_id)) => {
                if node_id == &self.myself {
                    RouteResult::Ok(self.myself.clone())
                } else {
                    RouteResult::Moved(slot, node_id.clone())
                }
            }
            Some(None) => RouteResult::Moved(slot, self.myself.clone()),
            None => RouteResult::Moved(slot, self.myself.clone()),
        }
    }

    /// Rebalance slots across master nodes using a round-robin approach.
    ///
    /// All slot ownership is cleared first, then slots are distributed
    /// evenly among masters.
    pub fn rebalance(&self) {
        let masters: Vec<String> = {
            let nodes = self.nodes.read();
            nodes
                .values()
                .filter(|n| n.is_master)
                .map(|n| n.node_id.clone())
                .collect()
        };

        if masters.is_empty() {
            return;
        }

        // Clear all slot assignments
        {
            let mut sm = self.slot_map.write();
            for entry in sm.iter_mut() {
                *entry = None;
            }
        }

        // Round-robin assign
        for (i, slot) in (0..HASH_SLOTS).enumerate() {
            let node_id = &masters[i % masters.len()];
            self.assign_slot(slot, node_id);
        }
    }

    pub fn get_slot_owner(&self, slot: u16) -> Option<String> {
        self.slot_map
            .read()
            .get(slot as usize)
            .and_then(|o| o.clone())
    }

    pub fn get_node_slots(&self, node_id: &str) -> Vec<u16> {
        self.nodes
            .read()
            .get(node_id)
            .map(|n| n.slots.clone())
            .unwrap_or_default()
    }

    /// Build a CLUSTER INFO style map for diagnostics.
    pub fn get_cluster_info(&self) -> HashMap<String, String> {
        let nodes = self.nodes.read();
        let mut info = HashMap::new();

        info.insert("cluster_state".into(), if self.check_slots_coverage() { "ok" } else { "fail" }.into());
        info.insert("cluster_slots_assigned".into(), self.count_assigned_slots().to_string());
        info.insert("cluster_slots_total".into(), HASH_SLOTS.to_string());
        info.insert("cluster_known_nodes".into(), nodes.len().to_string());

        for node in nodes.values() {
            let prefix = format!("node_{}", node.node_id);
            info.insert(format!("{prefix}_addr"), format!("{}:{}", node.addr, node.port));
            info.insert(format!("{prefix}_role"), if node.is_master { "master" } else { "slave" }.into());
            info.insert(format!("{prefix}_slots"), format!("{:?}", node.slots));
        }

        info
    }

    /// Check whether every slot is assigned to at least one node.
    pub fn check_slots_coverage(&self) -> bool {
        self.slot_map.read().iter().all(|o| o.is_some())
    }

    fn count_assigned_slots(&self) -> u16 {
        self.slot_map.read().iter().filter(|o| o.is_some()).count() as u16
    }

    /// Return the node_id of the local node.
    pub fn node_id(&self) -> &str {
        &self.myself
    }

    /// Return the list of all known node IDs.
    pub fn known_nodes(&self) -> Vec<String> {
        self.nodes.read().keys().cloned().collect()
    }

    /// Get the number of slots owned by a given node.
    pub fn slot_count(&self, node_id: &str) -> usize {
        self.nodes
            .read()
            .get(node_id)
            .map(|n| n.slots.len())
            .unwrap_or(0)
    }
}

/// CRC16 implementation matching Redis Cluster's XMODEM variant.
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
    crc16(effective) % HASH_SLOTS
}

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

    // ---------- CRC16 / hash_slot ----------

    #[test]
    fn crc16_empty_is_zero() {
        assert_eq!(crc16(b""), 0);
    }

    #[test]
    fn crc16_deterministic() {
        assert_eq!(crc16(b"hello"), crc16(b"hello"));
    }

    #[test]
    fn crc16_different_inputs_differ() {
        assert_ne!(crc16(b"abc"), crc16(b"xyz"));
    }

    #[test]
    fn hash_slot_within_bounds() {
        for key in [b"foo", b"bar", b"baz", b"", b"\xff\xff"] {
            let slot = hash_slot(key);
            assert!(slot < HASH_SLOTS, "slot {slot} out of range for key {key:?}");
        }
    }

    #[test]
    fn hash_slot_same_tag_same_slot() {
        let a = hash_slot(b"prefix{myslot}suffix1");
        let b = hash_slot(b"other{myslot}end");
        assert_eq!(a, b);
    }

    #[test]
    fn hash_slot_empty_braces_fallback() {
        let a = hash_slot(b"key{}rest");
        let b = hash_slot(b"key{}rest");
        assert_eq!(a, b);
    }

    // ---------- ClusterState ----------

    fn make_master(id: &str, addr: &str, port: u16, slots: Vec<u16>) -> ClusterNode {
        ClusterNode {
            node_id: id.into(),
            addr: addr.into(),
            port,
            slots,
            is_master: true,
            replicas: vec![],
        }
    }

    #[test]
    fn new_state_has_no_slot_ownership() {
        let cs = ClusterState::new("n1".into());
        assert!(!cs.check_slots_coverage());
        assert_eq!(cs.count_assigned_slots(), 0);
    }

    #[test]
    fn add_node_assigns_slots() {
        let cs = ClusterState::new("n1".into());
        let node = make_master("n1", "127.0.0.1", 7000, vec![0, 1, 2]);
        cs.add_node(node);

        assert_eq!(cs.get_slot_owner(0), Some("n1".into()));
        assert_eq!(cs.get_slot_owner(3), None);
        assert_eq!(cs.get_node_slots("n1"), vec![0, 1, 2]);
    }

    #[test]
    fn remove_node_clears_slots() {
        let cs = ClusterState::new("n1".into());
        let node = make_master("n1", "127.0.0.1", 7000, vec![0, 1, 2]);
        cs.add_node(node);
        cs.remove_node("n1");

        assert_eq!(cs.get_slot_owner(0), None);
        assert!(cs.get_node_slots("n1").is_empty());
    }

    #[test]
    fn assign_slot_updates_node() {
        let cs = ClusterState::new("n1".into());
        let node = make_master("n1", "127.0.0.1", 7000, vec![]);
        cs.add_node(node);

        cs.assign_slot(100, "n1");
        assert_eq!(cs.get_slot_owner(100), Some("n1".into()));
        assert!(cs.get_node_slots("n1").contains(&100));
    }

    #[test]
    fn route_key_ok_for_own_slot() {
        let cs = ClusterState::new("n1".into());
        let slot = hash_slot(b"mykey");
        let node = make_master("n1", "127.0.0.1", 7000, vec![slot]);
        cs.add_node(node);

        assert_eq!(cs.route_key(b"mykey"), RouteResult::Ok("n1".into()));
    }

    #[test]
    fn route_key_moved_for_other_node() {
        let cs = ClusterState::new("n1".into());
        let slot = hash_slot(b"mykey");

        let n1 = ClusterNode {
            node_id: "n1".into(),
            addr: "127.0.0.1".into(),
            port: 7000,
            slots: vec![],
            is_master: true,
            replicas: vec![],
        };
        let n2 = make_master("n2", "127.0.0.1", 7001, vec![slot]);
        cs.add_node(n1);
        cs.add_node(n2);

        assert_eq!(cs.route_key(b"mykey"), RouteResult::Moved(slot, "n2".into()));
    }

    #[test]
    fn route_key_unassigned_slot() {
        let cs = ClusterState::new("n1".into());
        // No nodes/slots assigned — should return Moved pointing to self
        let result = cs.route_key(b"anything");
        match result {
            RouteResult::Moved(slot, node_id) => {
                assert!(slot < HASH_SLOTS);
                assert_eq!(node_id, "n1");
            }
            _ => panic!("expected Moved for unassigned slot"),
        }
    }

    #[test]
    fn rebalance_distributes_evenly() {
        let cs = ClusterState::new("n1".into());
        let n1 = make_master("n1", "127.0.0.1", 7000, vec![]);
        let n2 = make_master("n2", "127.0.0.1", 7001, vec![]);
        cs.add_node(n1);
        cs.add_node(n2);

        cs.rebalance();

        assert!(cs.check_slots_coverage());

        let s1 = cs.slot_count("n1");
        let s2 = cs.slot_count("n2");
        assert_eq!(s1 + s2, HASH_SLOTS as usize);
        // With 2 nodes, difference at most 1
        assert!((s1 as i32 - s2 as i32).abs() <= 1);
    }

    #[test]
    fn rebalance_three_nodes() {
        let cs = ClusterState::new("n1".into());
        cs.add_node(make_master("n1", "127.0.0.1", 7000, vec![]));
        cs.add_node(make_master("n2", "127.0.0.1", 7001, vec![]));
        cs.add_node(make_master("n3", "127.0.0.1", 7002, vec![]));

        cs.rebalance();
        assert!(cs.check_slots_coverage());

        let total: usize = (0..HASH_SLOTS)
            .filter(|&s| cs.get_slot_owner(s).is_some())
            .count();
        assert_eq!(total, HASH_SLOTS as usize);
    }

    #[test]
    fn rebalance_no_masters_is_noop() {
        let cs = ClusterState::new("n1".into());
        cs.add_node(ClusterNode {
            node_id: "n1".into(),
            addr: "127.0.0.1".into(),
            port: 7000,
            slots: vec![],
            is_master: false,
            replicas: vec![],
        });
        cs.rebalance();
        assert!(!cs.check_slots_coverage());
    }

    #[test]
    fn get_cluster_info() {
        let cs = ClusterState::new("n1".into());
        cs.add_node(make_master("n1", "10.0.0.1", 7000, vec![0, 1]));
        cs.add_node(make_master("n2", "10.0.0.2", 7001, vec![2]));

        let info = cs.get_cluster_info();
        assert_eq!(info["cluster_state"], "ok");
        assert_eq!(info["cluster_slots_total"], "16384");
        assert_eq!(info["cluster_slots_assigned"], "3");
        assert_eq!(info["cluster_known_nodes"], "2");
    }

    #[test]
    fn check_slots_coverage_full() {
        let cs = ClusterState::new("n1".into());
        cs.add_node(make_master("n1", "127.0.0.1", 7000, vec![]));
        cs.rebalance();
        assert!(cs.check_slots_coverage());
    }

    #[test]
    fn node_id_accessor() {
        let cs = ClusterState::new("my-node".into());
        assert_eq!(cs.node_id(), "my-node");
    }

    #[test]
    fn known_nodes() {
        let cs = ClusterState::new("n1".into());
        cs.add_node(make_master("n1", "127.0.0.1", 7000, vec![]));
        cs.add_node(make_master("n2", "127.0.0.1", 7001, vec![]));

        let mut nodes = cs.known_nodes();
        nodes.sort();
        assert_eq!(nodes, vec!["n1", "n2"]);
    }
}
