//! Network node using an Add-Wins Set CRDT with commitment.
//!
//! This example wraps `VecLog<AWSet<String>>` in `CommitmentLog` and runs it
//! with `GenericCommitNode`. The network oracle probes up to three connected
//! peers every second and selects the smallest live replica ID as leader.
//! Leader updates are forwarded to the commitment log's internal oracle,
//! which adds a leader vote to each submitted operation automatically.
//!
//! Run the commands below from `moirai-commit-network`. The main thread receives
//! a json file path as an argument, used to read a json configuration file.
//! The json file should specify the replica ID, listening TCP port, listening
//! HTTP port (optional) and peer adress information. Examples of configurations
//! are given in "config_solo.json" and "config_a.json" - "config_b.json" for
//! the cases with a single replica and two replicas respectively.
//!
//! # Usage
//!
//! ```bash
//! # Single node
//! cargo run -p moirai-commit-network --example commit_awset_node examples/config_solo.json
//!
//! # Two-node cluster
//! cargo run -p moirai-commit-network --example commit_awset_node examples/config_a.json &
//! cargo run -p moirai-commit-network --example commit_awset_node examples/config_b.json &
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

extern crate serde_json;
use serde::Deserialize;
use std::fs::File;
use std::env;
use std::thread;
use std::time::Duration;

use moirai_crdt::set::aw_set::AWSet;
use moirai_network::composite::CompositeTransport;
use moirai_network::generic::GenericNode;
use moirai_commit_network::generic_commit_node::GenericCommitNode;
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

// Configuration file arguments
// replica_id: Arbitrary string, e.g. "a".
// listen_port: TCP listening port, e.g. 9001.
// http_port: HTTP listening port, e.g. 8081, this entry is optional.
// peers: Adresses of other replicas, if there are no other peers, an empty vector should be provided.
#[derive(Debug, Deserialize)]
struct Config {
    replica_id: String,
    listen_port: u16,
    http_port: Option<u16>,
    peers: Vec<String>,
}

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
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];
    let file = File::open(file_path).unwrap();
    let node_config: Config = serde_json::from_reader(file).expect("The file provided doesn't match the configuration specification.");

    // Parse PEERS=id:host:port,...
    let mut peer_addresses: HashMap<String, String> = HashMap::default();
    let mut all_members: Vec<String> = vec![node_config.replica_id.clone()];

    for spec in &node_config.peers {
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
        Omega::with_leader(node_config.replica_id.clone()),
        Resolver::default(),
    ));
    let transport = CompositeTransport::new(
        node_config.replica_id.clone(),
        node_config.listen_port,
        peer_addresses
    ).expect("Failed to create TCP transport");

    let mut node = GenericNode::with_transport_and_state(
        node_config.replica_id.clone(),
        &member_refs,
        transport,
        state,
    );

    // Enable state querying - requires QueryableLog trait implementation
    node.enable_state_query();

    // Start HTTP API if port is specified
    if let Some(port) = node_config.http_port {
        node.start_http(port);
        eprintln!(
            "[{}] HTTP API listening on http://0.0.0.0:{}",
            &node_config.replica_id, port
        );
    }

    // Give peers time to start, then connect
    thread::sleep(Duration::from_secs(2));
    node.connect();

    eprintln!(
        "[{}] Commitment AWSet node running. Submit ops to http://localhost:{}/api/op",
        node_config.replica_id,
        node_config.http_port.unwrap_or(0)
    );
    eprintln!(
        "[{}] Query state at http://localhost:{}/api/state",
        node_config.replica_id,
        node_config.http_port.unwrap_or(0)
    );
    eprintln!("[{}] Example: curl -X POST http://localhost:{}/api/op -H 'Content-Type: application/json' -d '{{\"Add\":\"apple\"}}'",
        node_config.replica_id, node_config.http_port.unwrap_or(0));

    // Run the event loop
    let mut node = GenericCommitNode::new(node, Duration::from_secs(10), 1);
    node.run_with_oracle(|replica, network_omega| {
        *replica.state_mut().0.oracle_mut() = network_omega
            .query()
            .map(Omega::with_leader)
            .unwrap_or_default();
    });
}
