//! Integration checks for the commitment log APIs over the network wire format.
use moirai_crdt::{
    json::{Json, JsonLog},
    list::{eg_walker::List, nested_list::NestedList},
    map::uw_map::UWMap,
};
use moirai_network::state_transfer::TransferableLog;
use moirai_protocol::{
    broadcast::{message::EventMessage, tcsb::Tcsb},
    crdt::query::Read,
    replica::{IsReplica, Replica},
};

type JsonReplica = Replica<JsonLog, Tcsb<Json>>;

fn wire<T: serde::Serialize + serde::de::DeserializeOwned>(value: &T) -> T {
    serde_json::from_slice(&serde_json::to_vec(value).unwrap()).unwrap()
}

#[test]
fn nested_json_log_transfers_and_accepts_follow_up_operations() {
    let mut donor = JsonReplica::bootstrap("a".into(), &["a", "b"]);
    let mut peer = JsonReplica::bootstrap("b".into(), &["b", "a"]);
    let commands = [
        Json::Object(UWMap::Update(
            "text".into(),
            Box::new(Json::String(List::Insert {
                content: 'a',
                pos: 0,
            })),
        )),
        Json::Object(UWMap::Update(
            "array".into(),
            Box::new(Json::Array(NestedList::Insert {
                pos: 0,
                op: Box::new(Json::Boolean(moirai_crdt::flag::ew_flag::EWFlag::Enable)),
            })),
        )),
    ];
    for command in commands {
        let message = donor.send(command).unwrap();
        peer.receive(wire::<EventMessage<Json>>(&message));
    }
    assert_eq!(donor.query(Read::new()), peer.query(Read::new()));
    // Fail loudly rather than letting TransferableLog's null fallback hide a
    // serializer that cannot encode a populated log (notably EventId map keys).
    let encoded = serde_json::to_value(donor.log()).unwrap();
    let state = JsonLog::import_log(encoded).unwrap();
    let mut joiner = JsonReplica::bootstrap("c".into(), &["c"]);
    joiner.adopt(wire(&donor.snapshot()), state);
    assert_eq!(donor.query(Read::new()), joiner.query(Read::new()));
    let next = joiner
        .send(Json::Object(UWMap::Update(
            "text".into(),
            Box::new(Json::String(List::Insert {
                content: 'b',
                pos: 1,
            })),
        )))
        .unwrap();
    donor.receive(wire(&next));
    peer.receive(wire(&next));
    assert_eq!(donor.query(Read::new()), joiner.query(Read::new()));
    assert_eq!(peer.query(Read::new()), joiner.query(Read::new()));
}

#[test]
fn commitment_commands_replicate_with_their_leader_votes() {
    use moirai_crdt::set::aw_set::AWSet;
    use moirai_protocol::{
        commitment::{commit_log::CommitmentLog, commit_op::CommitOp},
        state::po_log::VecLog,
    };
    type Log = CommitmentLog<VecLog<AWSet<String>>>;
    type CommittedReplica = Replica<Log, Tcsb<CommitOp<AWSet<String>>>>;
    fn network_op<O: moirai_network::generic::NetworkOp>() {}
    network_op::<CommitOp<AWSet<String>>>();
    let mut a = CommittedReplica::bootstrap("a".into(), &["a", "b", "c"]);
    let mut b = CommittedReplica::bootstrap("b".into(), &["b", "a", "c"]);
    a.state_mut().oracle_mut().set_leader("a");
    b.state_mut().oracle_mut().set_leader("a");
    let first = a.send(AWSet::Add("apple".into())).unwrap();
    assert_eq!(first.event().op().vote.id(), "a");
    b.receive(wire(&first));
    let concurrent_a = a.send(AWSet::Add("banana".into())).unwrap();
    let concurrent_b = b.send(AWSet::Remove("apple".into())).unwrap();
    a.receive(wire(&concurrent_b));
    b.receive(wire(&concurrent_a));
    assert_eq!(a.query(Read::new()), b.query(Read::new()));
    assert_eq!(
        a.query(Read::new()).into_iter().collect::<Vec<_>>(),
        vec!["banana"]
    );
}

#[test]
fn cached_set_round_trips_without_persisting_its_cache() {
    use moirai_crdt::set::aw_set::AWSet;
    use moirai_protocol::{crdt::eval::BorrowedRead, state::po_log::VecLog};
    type Log = VecLog<AWSet<String>>;
    let mut replica = Replica::<Log, Tcsb<AWSet<String>>>::bootstrap("a".into(), &["a", "b"]);
    replica.send(AWSet::Add("apple".into())).unwrap();
    let value = replica.log().read_ref().clone();
    let encoded = serde_json::to_value(replica.log()).unwrap();
    assert!(encoded.get("read_cache").is_none());
    assert!(encoded.get("unstable").is_some());
    let restored: Log = serde_json::from_value(encoded).unwrap();
    assert_eq!(restored.read_ref(), &value);
}
