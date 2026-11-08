//! The protocol crate and the worker name one protocol contract, so protocols
//! run on a worker without an adapter.

/// Echo one request back through `protocol` using only the worker's trait
/// bound. The request bytes go to `buffer`, which the outcome borrows.
fn echo_round_trip<'a, P: xylem_core::threading::Protocol>(
    protocol: &mut P,
    buffer: &'a mut Vec<u8>,
) -> (P::RequestId, xylem_core::threading::Parsed<'a, P::RequestId>) {
    let request = protocol.next_request(3);
    *buffer = request.data;
    let data: &'a Vec<u8> = buffer;
    (request.request_id, protocol.parse_response(3, data).unwrap())
}

#[test]
fn protocol_reexports_are_one_type() {
    // A protocol implemented against xylem_protocols satisfies the worker's bound.
    let mut echo = xylem_protocols::XylemEchoProtocol::default();
    let mut buffer = Vec::new();
    let (sent_id, parsed) = echo_round_trip(&mut echo, &mut buffer);
    assert_eq!(sent_id, (3, 0));
    assert_eq!(parsed, xylem_protocols::Parsed::ok(16, (3, 0)));
}
