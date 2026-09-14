//! Network node using an Add-Wins Set CRDT with commitment.
//!
//! This example wraps `VecLog<AWSet<String>>` in `CommitmentLog` and runs it
//! with `GenericCommitNode`. The network oracle probes up to three connected
//! peers every second and selects the smallest live replica ID as leader.
//! Leader updates are forwarded to the commitment log's internal oracle,
//! which adds a leader vote to each submitted operation automatically.
//!
//! Run the commands below from the workspace root. `REPLICA_ID` defaults to
//! `replica-a`, and `LISTEN_PORT` defaults to `9001`. Set `HTTP_PORT` to enable
//! the HTTP API. `PEERS` is a comma-separated list of `id:host:port` entries;
//! list every other cluster member and use a unique ID and ports for each node.
//! Wait for the nodes to enter their event loops before submitting operations.
//!
//! # Usage
//!
//! ```bash
//! # Single node
//! REPLICA_ID=a LISTEN_PORT=9001 HTTP_PORT=8081 \
//!     cargo run -p moirai-network --example commit_awset_node
//!
//! # Two-node cluster
//! REPLICA_ID=a LISTEN_PORT=9001 HTTP_PORT=8081 PEERS=b:127.0.0.1:9002 \
//!     cargo run -p moirai-network --example commit_awset_node &
//! REPLICA_ID=b LISTEN_PORT=9002 HTTP_PORT=8082 PEERS=a:127.0.0.1:9001 \
//!     cargo run -p moirai-network --example commit_awset_node &
//! ```
//!
//! # HTTP API
//!
//! - `POST /api/op`        — submit an operation as JSON
//! - `GET  /api/state`     — query the current set contents
//! - `GET  /api/operations` — query all delivered operations
//! - `GET  /api/health`    — health check
//! - `GET  /api/peers`     — list connected peers
//! - `POST /api/pause/<id>`  — pause connection to a peer
//! - `POST /api/resume/<id>` — resume connection and sync
//!
//! # Example Operations
//!
//! ```bash
//! # Add an element
//! curl -X POST http://localhost:8081/api/op \
//!   -H "Content-Type: application/json" \
//!   -d '{"Add":"apple"}'
//!
//! # Add another element
//! curl -X POST http://localhost:8081/api/op \
//!   -H "Content-Type: application/json" \
//!   -d '{"Add":"banana"}'
//!
//! # Remove an element
//! curl -X POST http://localhost:8081/api/op \
//!   -H "Content-Type: application/json" \
//!   -d '{"Remove":"apple"}'
//!
//! # Clear the set
//! curl -X POST http://localhost:8081/api/op \
//!   -H "Content-Type: application/json" \
//!   -d '"Clear"'
//!
//! # Query the set contents (computed state)
//! curl http://localhost:8081/api/state
//!
//! # Or query operations log
//! curl http://localhost:8081/api/operations
//! ```

use std::env;
use std::thread;
use std::time::Duration;

use moirai_crdt::set::aw_set::AWSet;
use moirai_network::composite::CompositeTransport;
use moirai_network::generic::GenericNode;
use moirai_network::generic_commit_node::GenericCommitNode;
use moirai_protocol::broadcast::internalizer::Resolver;
use moirai_protocol::commitment::commit_log::CommitmentLog;
use moirai_protocol::commitment::oracle::{IsOracle, Omega};
use moirai_network::HashMap;
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::clock::version_vector::Version;
use moirai_protocol::crdt::query::Read;
use moirai_protocol::event::Event;
use moirai_protocol::replica::{IsReplica, Replica};
use moirai_protocol::state::log::IsLog;
use moirai_protocol::state::po_log::VecLog;

// Newtype wrapper to implement QueryableLog (Rust orphan rules require a local type)
#[derive(Debug, Clone, Default)]
struct AWSetLog(CommitmentLog<VecLog<AWSet<String>>>);

// Delegate all IsLog methods to the inner VecLog
impl IsLog for AWSetLog {
    type Value = <CommitmentLog<VecLog<AWSet<String>>> as IsLog>::Value;
    type Op = <CommitmentLog<VecLog<AWSet<String>>> as IsLog>::Op;

    type Command = <CommitmentLog<VecLog<AWSet<String>>> as IsLog>::Command;
    type Rejection = <CommitmentLog<VecLog<AWSet<String>>> as IsLog>::Rejection;

    fn prepare(&self, command: Self::Command) -> Self::Op {
        self.0.prepare(command)
    }

    fn is_enabled(&self, op: &Self::Op) -> Result<(), Self::Rejection> {
        self.0.is_enabled(op)
    }

    fn effect(
        &mut self,
        event: Event<Self::Op>,
        ctx: &mut moirai_protocol::state::effect_context::EffectContext<'_>,
    ) {
        self.0.effect(event, ctx)
    }

    fn stabilize(&mut self, version: &Version) {
        self.0.stabilize(version)
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        self.0.redundant_by_parent(version, conservative)
    }

    fn is_default(&self) -> bool {
        self.0.is_default()
    }
}

// Delegate query execution to inner VecLog
impl<Q> moirai_protocol::crdt::eval::EvalNested<Q> for AWSetLog
where
    CommitmentLog<VecLog<AWSet<String>>>: moirai_protocol::crdt::eval::EvalNested<Q>,
    Q: moirai_protocol::crdt::query::QueryOperation,
{
    fn execute_query(&self, q: Q) -> Q::Response {
        self.0.execute_query(q)
    }
}

// Implement QueryableLog to enable GET /api/state endpoint
impl moirai_network::query::QueryableLog for AWSetLog {
    fn query_state_json(replica: &Replica<Self, Tcsb<Self::Op>>) -> serde_json::Value {
        let value = replica.query(Read::new());
        serde_json::to_value(&value).unwrap_or_else(
            |e| serde_json::json!({ "error": format!("Failed to serialize state: {}", e) }),
        )
    }
}

fn main() {
    let replica_id = env::var("REPLICA_ID").unwrap_or_else(|_| "replica-a".to_string());
    let listen_port: u16 = env::var("LISTEN_PORT")
        .unwrap_or_else(|_| "9001".to_string())
        .parse()
        .expect("Invalid LISTEN_PORT");
    let http_port: Option<u16> = env::var("HTTP_PORT").ok().and_then(|p| p.parse().ok());
    let peers_str = env::var("PEERS").unwrap_or_default();

    // Parse PEERS=id:host:port,...
    let mut peer_addresses: HashMap<String, String> = HashMap::default();
    let mut all_members: Vec<String> = vec![replica_id.clone()];
    for spec in peers_str.split(',').filter(|s| !s.is_empty()) {
        let parts: Vec<&str> = spec.split(':').collect();
        if parts.len() >= 3 {
            let peer_id = parts[0].to_string();
            let addr = format!("{}:{}", parts[1], parts[2]);
            all_members.push(peer_id.clone());
            peer_addresses.insert(peer_id, addr);
        }
    }

    let member_refs: Vec<&str> = all_members.iter().map(|s| s.as_str()).collect();

    let state = AWSetLog(CommitmentLog::new(
        VecLog::default(),
        Omega::with_leader(replica_id.clone()),
        Resolver::default(),
    ));
    let transport = CompositeTransport::new(replica_id.clone(), listen_port, peer_addresses)
        .expect("Failed to create TCP transport");
    let mut node = GenericNode::with_transport_and_state(
        replica_id.clone(),
        &member_refs,
        transport,
        state,
    );

    // Enable state querying - requires QueryableLog trait implementation
    node.enable_state_query();

    // Start HTTP API if port is specified
    if let Some(port) = http_port {
        node.start_http(port);
        eprintln!(
            "[{}] HTTP API listening on http://0.0.0.0:{}",
            replica_id, port
        );
    }

    // Give peers time to start, then connect
    thread::sleep(Duration::from_secs(2));
    node.connect();

    eprintln!(
        "[{}] Commitment AWSet node running. Submit ops to http://localhost:{}/api/op",
        replica_id,
        http_port.unwrap_or(0)
    );
    eprintln!(
        "[{}] Query state at http://localhost:{}/api/state",
        replica_id,
        http_port.unwrap_or(0)
    );
    eprintln!("[{}] Example: curl -X POST http://localhost:{}/api/op -H 'Content-Type: application/json' -d '{{\"Add\":\"apple\"}}'",
        replica_id, http_port.unwrap_or(0));

    // Run the event loop
    let mut node = GenericCommitNode::new(node, Duration::from_secs(1), 3);
    node.run_with_oracle(|replica, network_omega| {
        *replica.state_mut().0.oracle_mut() = network_omega
            .query()
            .map(Omega::with_leader)
            .unwrap_or_default();
    });
}
