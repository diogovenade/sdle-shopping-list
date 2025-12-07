use std::collections::{BTreeMap, HashSet};
use uuid::Uuid;
use md5::{Md5, Digest};

pub struct HashRing {
    ring: BTreeMap<u128, Uuid>, // hash -> server_id
    virtual_nodes: usize, // partitioning
    replicas: usize, // replication
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
            let hash = self.hash(&key);
            self.ring.insert(hash, node_id);
        }
    }

    pub fn remove_node(&mut self, node_id: Uuid) {
        for i in 0..self.virtual_nodes {
            let key = format!("{}-{}", node_id, i);
            let hash = self.hash(&key);
            self.ring.remove(&hash);
        }
    }

    pub fn get_preference_list(&self, list_id: &Uuid) -> Vec<Uuid> {
        if self.ring.is_empty() {
            return vec![];
        }

        let hash = self.hash(&list_id.to_string());
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

    fn hash(&self, key: &str) -> u128 {
        let mut hasher = Md5::new();
        hasher.update(key.as_bytes());
        let result = hasher.finalize();
        u128::from_be_bytes(result.into())
    }
}