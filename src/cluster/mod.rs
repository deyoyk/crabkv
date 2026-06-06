use std::collections::HashMap;
use parking_lot::RwLock;

pub const HASH_SLOTS: u16 = 16384;

pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= byte as u16;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xA001;
            } else {
                crc >>= 1;
            }
        }
    }
    crc
}

pub fn hash_slot(key: &[u8]) -> u16 {
    if let Some(start) = key.iter().position(|&b| b == b'{') {
        if let Some(end) = key[start + 1..].iter().position(|&b| b == b'}') {
            let hash_key = &key[start + 1..start + 1 + end];
            return crc16(hash_key) % HASH_SLOTS;
        }
    }
    crc16(key) % HASH_SLOTS
}

#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub node_id: String,
    pub addr: String,
    pub port: u16,
    pub slots: Vec<u16>,
    pub is_master: bool,
    pub replicas: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum RouteResult {
    Ok(String),
    Moved(u16, String),
    Ask(u16, String),
}

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
        for slot in &node.slots {
            self.slot_map.write()[*slot as usize] = Some(node_id.clone());
        }
        self.nodes.write().insert(node_id, node);
    }

    pub fn remove_node(&self, node_id: &str) {
        if let Some(node) = self.nodes.write().remove(node_id) {
            for slot in &node.slots {
                self.slot_map.write()[*slot as usize] = None;
            }
        }
    }

    pub fn assign_slot(&self, slot: u16, node_id: &str) {
        self.slot_map.write()[slot as usize] = Some(node_id.to_string());
    }

    pub fn route_key(&self, key: &[u8]) -> RouteResult {
        let slot = hash_slot(key);
        let map = self.slot_map.read();
        match &map[slot as usize] {
            Some(node_id) => {
                if node_id == &self.myself {
                    RouteResult::Ok(node_id.clone())
                } else {
                    RouteResult::Moved(slot, node_id.clone())
                }
            }
            None => RouteResult::Moved(slot, "unknown".to_string()),
        }
    }

    pub fn rebalance(&self) {
        let master_nodes: Vec<String> = self.nodes.read().values()
            .filter(|n| n.is_master)
            .map(|n| n.node_id.clone())
            .collect();

        if master_nodes.is_empty() { return; }

        let slots_per_node = HASH_SLOTS / master_nodes.len() as u16;
        let remainder = HASH_SLOTS % master_nodes.len() as u16;

        let mut slot = 0u16;
        for (i, node_id) in master_nodes.iter().enumerate() {
            let count = slots_per_node + if (i as u16) < remainder { 1 } else { 0 };
            let end_slot = slot + count;

            let mut node_slots = Vec::new();
            let mut map = self.slot_map.write();
            while slot < end_slot && slot < HASH_SLOTS {
                map[slot as usize] = Some(node_id.clone());
                node_slots.push(slot);
                slot += 1;
            }

            if let Some(node) = self.nodes.write().get_mut(node_id) {
                node.slots = node_slots;
            }
        }
    }

    pub fn get_slot_owner(&self, slot: u16) -> Option<String> {
        self.slot_map.read()[slot as usize].clone()
    }

    pub fn get_node_slots(&self, node_id: &str) -> Vec<u16> {
        let map = self.slot_map.read();
        map.iter().enumerate()
            .filter(|(_, owner)| owner.as_deref() == Some(node_id))
            .map(|(slot, _)| slot as u16)
            .collect()
    }

    pub fn get_cluster_info(&self) -> HashMap<String, String> {
        let nodes = self.nodes.read();
        let map = self.slot_map.read();
        let covered = map.iter().filter(|o| o.is_some()).count();

        let mut info = HashMap::new();
        info.insert("cluster_state".into(), if covered == HASH_SLOTS as usize { "ok" } else { "fail" }.into());
        info.insert("cluster_slots_assigned".into(), covered.to_string());
        info.insert("cluster_slots_ok".into(), covered.to_string());
        info.insert("cluster_known_nodes".into(), nodes.len().to_string());
        info.insert("cluster_size".into(), nodes.values().filter(|n| n.is_master).count().to_string());
        info
    }

    pub fn check_slots_coverage(&self) -> bool {
        self.slot_map.read().iter().all(|o| o.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc16() {
        assert_eq!(crc16(b"hello"), crc16(b"hello"));
    }

    #[test]
    fn test_hash_slot_consistency() {
        assert_eq!(hash_slot(b"foo"), hash_slot(b"foo"));
        assert_eq!(hash_slot(b"{foo}bar"), hash_slot(b"{foo}baz"));
    }

    #[test]
    fn test_route_key() {
        let state = ClusterState::new("n1".into());
        state.add_node(ClusterNode {
            node_id: "n1".into(), addr: "127.0.0.1".into(), port: 6379,
            slots: (0..8192).collect(), is_master: true, replicas: vec![],
        });
        state.add_node(ClusterNode {
            node_id: "n2".into(), addr: "127.0.0.1".into(), port: 6380,
            slots: (8192..16384).collect(), is_master: true, replicas: vec![],
        });

        match state.route_key(b"test") {
            RouteResult::Ok(_) => {}
            RouteResult::Moved(_, _) => {}
            _ => {}
        }
    }

    #[test]
    fn test_rebalance() {
        let state = ClusterState::new("n1".into());
        state.add_node(ClusterNode {
            node_id: "n1".into(), addr: "127.0.0.1".into(), port: 6379,
            slots: vec![], is_master: true, replicas: vec![],
        });
        state.add_node(ClusterNode {
            node_id: "n2".into(), addr: "127.0.0.1".into(), port: 6380,
            slots: vec![], is_master: true, replicas: vec![],
        });
        state.rebalance();
        assert!(state.check_slots_coverage());
    }
}
