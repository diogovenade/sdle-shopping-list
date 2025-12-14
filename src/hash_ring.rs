use md5::{Digest, Md5};
use std::collections::{BTreeMap, HashSet};
use uuid::Uuid;

pub const VNODES: usize = 3;
pub const REPLICAS: usize = 3;
pub const WRITE_NODES: usize = 2; // minimum number of nodes that must participate in a write
pub const READ_NODES: usize = 2; // minimum number of nodes that must participate in a read

pub struct HashRing {
    ring: BTreeMap<u128, Uuid>, // hash -> server_id
    virtual_nodes: usize,       // partitioning
    replicas: usize,            // replication
}

impl HashRing {
    pub fn new(virtual_nodes: usize, replicas: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            virtual_nodes,
            replicas,
        }
    }

    pub fn add_node(&mut self, node_id: Uuid) {
        for i in 0..self.virtual_nodes {
            let key = format!("{}-{}", node_id, i);
            let hash = HashRing::hash(&key);
            self.ring.insert(hash, node_id);
        }
    }

    pub fn remove_node(&mut self, node_id: Uuid) {
        for i in 0..self.virtual_nodes {
            let key = format!("{}-{}", node_id, i);
            let hash = HashRing::hash(&key);
            self.ring.remove(&hash);
        }
    }

    pub fn get_preference_list(&self, list_id: &Uuid) -> Vec<Uuid> {
        if self.ring.is_empty() {
            return vec![];
        }

        let hash = HashRing::hash(&list_id.to_string());
        let mut preference_list = Vec::new();
        let mut seen_servers = HashSet::new();

        let iter = self.ring.range(hash..).chain(self.ring.iter());

        for (_, node_id) in iter {
            // Only add distinct physical nodes
            if !seen_servers.contains(node_id) {
                preference_list.push(*node_id);
                seen_servers.insert(*node_id);

                if preference_list.len() >= self.replicas {
                    break;
                }
            }
        }

        preference_list
    }

    pub fn get_coordinator(&self, list_id: &Uuid) -> Option<Uuid> {
        self.get_preference_list(list_id).first().copied()
    }

    pub fn get_all_servers(&self) -> HashSet<Uuid> {
        self.ring.values().copied().collect()
    }

    pub fn server_count(&self) -> usize {
        self.get_all_servers().len()
    }

    pub fn hash(key: &str) -> u128 {
        let mut hasher = Md5::new();
        hasher.update(key.as_bytes());
        let result = hasher.finalize();
        u128::from_be_bytes(result.into())
    }

    pub fn next_node(&self, node_id: &Uuid) -> Option<Uuid> {
        if self.ring.is_empty() {
            return None;
        }

        // Collect all virtual node hashes for this physical node
        let mut vnodes: Vec<u128> = self
            .ring
            .iter()
            .filter_map(|(hash, id)| if id == node_id { Some(*hash) } else { None })
            .collect();

        if vnodes.is_empty() {
            return None;
        }

        vnodes.sort_unstable();
        let start = vnodes[0];

        let iter = self.ring.range((start + 1)..).chain(self.ring.iter());

        for (_, next_id) in iter {
            if next_id != node_id {
                return Some(*next_id);
            }
        }

        None
    }

    pub fn print_ring(&self) {
        let nodes: Vec<String> = self
            .ring
            .values()
            .map(|id| id.to_string()[..8].to_string())
            .collect();

        println!("HASH-RING: {}", nodes.join(" -> "));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preference_list_returns_n_servers() {
        let mut ring = HashRing::new(3, 3);

        let node1 = Uuid::new_v4();
        let node2 = Uuid::new_v4();
        let node3 = Uuid::new_v4();
        let node4 = Uuid::new_v4();

        ring.add_node(node1);
        ring.add_node(node2);
        ring.add_node(node3);
        ring.add_node(node4);

        let list_id = Uuid::new_v4();
        let preference_list = ring.get_preference_list(&list_id);

        assert_eq!(preference_list.len(), 3);

        let unique: HashSet<_> = preference_list.iter().collect();
        assert_eq!(unique.len(), 3);
    }

    #[test]
    fn test_coordinator_is_first_in_preference_list() {
        let mut ring = HashRing::new(5, 3);

        let node1 = Uuid::new_v4();
        let node2 = Uuid::new_v4();

        ring.add_node(node1);
        ring.add_node(node2);

        let list_id = Uuid::new_v4();
        let coordinator = ring.get_coordinator(&list_id);
        let preference_list = ring.get_preference_list(&list_id);

        assert_eq!(coordinator, preference_list.first().copied());
    }

    #[test]
    fn test_remove_server() {
        let mut ring = HashRing::new(3, 2);

        let node1 = Uuid::new_v4();
        let node2 = Uuid::new_v4();

        ring.add_node(node1);
        ring.add_node(node2);

        assert_eq!(ring.server_count(), 2);

        ring.remove_node(node1);

        assert_eq!(ring.server_count(), 1);
        assert!(ring.get_all_servers().contains(&node2));
        assert!(!ring.get_all_servers().contains(&node1));
    }

    #[test]
    fn test_handles_fewer_servers_than_replication_factor() {
        let mut ring = HashRing::new(3, 5);

        let node1 = Uuid::new_v4();
        let node2 = Uuid::new_v4();

        ring.add_node(node1);
        ring.add_node(node2);

        let list_id = Uuid::new_v4();
        let preference_list = ring.get_preference_list(&list_id);

        assert_eq!(preference_list.len(), 2);
    }
}
