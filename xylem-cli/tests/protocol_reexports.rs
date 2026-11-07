//! The protocol crate and the worker name one protocol contract, so protocols
//! run on a worker without an adapter.

/// Echo one request back through `protocol` using only the worker's trait bound.
fn echo_round_trip<P: xylem_core::threading::Protocol>(
    protocol: &mut P,
) -> (P::RequestId, (usize, Option<P::RequestId>)) {
    let request = protocol.next_request(3);
    let parsed = protocol.parse_response(3, &request.data).unwrap();
    (request.request_id, parsed)
}

#[test]
fn protocol_reexports_are_one_type() {
    // A protocol implemented against xylem_protocols satisfies the worker's bound.
    let mut echo = xylem_protocols::XylemEchoProtocol::default();
    let (sent_id, parsed) = echo_round_trip(&mut echo);
    assert_eq!(sent_id, (3, 0));
    assert_eq!(parsed, (16, Some((3, 0))));
}
