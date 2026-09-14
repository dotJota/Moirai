use moirai_protocol::commitment::oracle::IsOracle;
use moirai_protocol::replica::ReplicaIdOwned;
use moirai_protocol::utils::hashmap::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicaStatus {
    Up,
    Down,
}

impl Default for ReplicaStatus {
    fn default() -> Self { ReplicaStatus::Up }
}

/// Selects the lexicographically smallest connected replica.
#[derive(Clone, Copy, Debug, Default)]
pub struct Resolver;

impl Resolver {
    pub fn resolve(
        &self,
        peers: &HashMap<ReplicaIdOwned, ReplicaStatus>,
    ) -> Option<ReplicaIdOwned> {
        peers
            .iter()
            .filter_map(|(id, status)| (*status == ReplicaStatus::Up).then_some(id))
            .min()
            .cloned()
    }
}

/// An eventual leader oracle driven by the locally observed connectivity state.
/// Include the local replica as `Up` if it should participate in the election.
#[derive(Clone, Debug, Default)]
pub struct Omega {
    resolver: Resolver,
    peers: HashMap<ReplicaIdOwned, ReplicaStatus>,
}

impl Omega {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_peer_status(&mut self, peer: impl Into<ReplicaIdOwned>, status: ReplicaStatus) {
        self.peers.insert(peer.into(), status);
    }

    /// Replace the current peers with new, updated ones. It resets the status of every replica to UP.
    pub fn update_peers(&mut self, peers: &[ReplicaIdOwned]) {
        self.peers.clear();
        for replica in peers {
            self.set_peer_status(replica, ReplicaStatus::Up);
        }
    }
}

impl IsOracle for Omega {
    fn query(&self) -> Option<ReplicaIdOwned> {
        self.resolver.resolve(&self.peers)
    }
}
