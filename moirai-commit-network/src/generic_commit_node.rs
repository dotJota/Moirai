use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::time::{Duration, Instant};

use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::replica::{IsReplica, Replica};
use moirai_protocol::state::log::IsLog;

use moirai_network::generic::{GenericNode, NetworkOp};
use crate::network_omega::{Omega, ReplicaStatus};
use moirai_network::transport::{CrdtTransport, PeerStatus, TransportMessage};
use moirai_network::HashSet;

/// A network node with periodic, randomly sampled peer liveness checks.
pub struct GenericCommitNode<L: IsLog, T: CrdtTransport<Op = L::Op>>
where
    L::Op: NetworkOp,
    L::Command: NetworkOp,
{
    pub generic_node: GenericNode<L, T>,
    pub network_omega: Omega,
    period: Duration,
    peer_count: usize,
}

impl<L: IsLog, T: CrdtTransport<Op = L::Op>> GenericCommitNode<L, T>
where
    L::Op: NetworkOp,
    L::Command: NetworkOp,
{
    /// Probe at most `peer_count` distinct connected peers every `period`.
    /// A probe without a batch reply by the next round marks its peer down.
    /// Panics if `period` is zero.
    pub fn new(generic_node: GenericNode<L, T>, period: Duration, peer_count: usize) -> Self {
        assert!(!period.is_zero(), "probe period must be nonzero");
        Self {
            generic_node,
            network_omega: Omega::new(),
            period,
            peer_count,
        }
    }

    pub fn run(&mut self) {
        self.run_with_oracle(|_, _| {});
    }

    /// Forward the current network oracle after each liveness update.
    pub fn run_with_oracle<F>(&mut self, mut update: F)
    where
        F: FnMut(&mut Replica<L, Tcsb<L::Op>>, &Omega),
    {
        let omega = &mut self.network_omega;
        let period = self.period;
        let peer_count = self.peer_count;
        let mut pending = HashSet::default();
        let mut known = HashSet::default();
        let mut last_probe: Option<Instant> = None;
        let random = RandomState::new();
        let mut draw = 0_u64;

        self.generic_node
            .run_with_observer(|replica, transport, incoming| {
                if let Some((from, message)) = incoming {
                    // SyncRequest is answered with Batch, including an empty batch.
                    // There are no request IDs; any batch is evidence of liveness.
                    if matches!(message, TransportMessage::Batch { .. }) {
                        pending.remove(from);
                        omega.set_peer_status(from, ReplicaStatus::Up);
                    }
                    update(replica, omega);
                    // Return only from the observer; GenericNode still handles
                    // the message and applies its batch to the local replica.
                    return;
                }

                let now = Instant::now();
                if last_probe.is_some_and(|last| now.duration_since(last) < period) {
                    return;
                }
                for peer in pending.drain() {
                    // Forget the outstanding probe, not the peer or its state.
                    omega.set_peer_status(peer, ReplicaStatus::Down);
                }
                omega.set_peer_status(transport.local_id(), ReplicaStatus::Up);

                let peers = transport.peers();
                let current: HashSet<_> = peers.iter().map(|peer| peer.id.clone()).collect();
                for removed in known.difference(&current) {
                    omega.set_peer_status(removed, ReplicaStatus::Down);
                }
                let mut candidates = Vec::with_capacity(peers.len());
                for peer in peers {
                    if peer.id == *transport.local_id() {
                        continue;
                    }
                    if peer.status != PeerStatus::Connected {
                        omega.set_peer_status(peer.id, ReplicaStatus::Down);
                    } else {
                        if !known.contains(&peer.id) {
                            omega.set_peer_status(&peer.id, ReplicaStatus::Up);
                        }
                        candidates.push(peer.id);
                    }
                }
                known = current;

                // A partial Fisher-Yates shuffle samples without replacement.
                let count = peer_count.min(candidates.len());
                if count > 0 {
                    let message = TransportMessage::SyncRequest {
                        since: replica.since(),
                    };
                    for i in 0..count {
                        let remaining = (candidates.len() - i) as u64;
                        let threshold = remaining.wrapping_neg() % remaining;
                        let value = loop {
                            let value = random.hash_one(draw);
                            draw = draw.wrapping_add(1);
                            if value >= threshold {
                                break value;
                            }
                        };
                        candidates.swap(i, i + (value % remaining) as usize);
                        let peer = &candidates[i];
                        if transport.send(peer, message.clone()).is_ok() {
                            pending.insert(peer.clone());
                        } else {
                            omega.set_peer_status(peer, ReplicaStatus::Down);
                        }
                    }
                }
                update(replica, omega);
                last_probe = Some(Instant::now());
            });
    }
}
