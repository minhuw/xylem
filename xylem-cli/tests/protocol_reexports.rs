//! The protocol crate and the worker name one protocol contract, so protocols
//! run on a worker without an adapter.

/// Echo one request back through `protocol` using only the worker's trait bound.
fn echo_round_trip<P: xylem_core::threading::Protocol>(
    protocol: &mut P,
) -> (P::RequestId, xylem_core::threading::ParseResult<P::RequestId>) {
    let request = protocol.next_request(3);
    let parsed = protocol.parse_response_extended(3, &request.data).unwrap();
    (request.request_id, parsed)
}

#[test]
fn protocol_reexports_are_one_type() {
    // A protocol implemented against xylem_protocols satisfies the worker's bound,
    // and the worker's ParseResult binds to the protocol crate's ParseResult.
    let mut echo = xylem_protocols::XylemEchoProtocol::default();
    let (sent_id, parsed): (_, xylem_protocols::ParseResult<(usize, u64)>) =
        echo_round_trip(&mut echo);
    assert_eq!(sent_id, (3, 0));
    assert!(matches!(
        parsed,
        xylem_protocols::ParseResult::Complete { bytes_consumed: 16, request_id: (3, 0) }
    ));

    // Values built through either path bind to variables typed with the other.
    let retry: xylem_core::threading::RetryRequest<u64> = xylem_protocols::RetryRequest {
        bytes_consumed: 8,
        original_request_id: 7,
        is_warmup: false,
        target_conn_id: Some(1),
        prepare_commands: vec![b"*1\r\n$6\r\nASKING\r\n".to_vec()],
        attempt: 0,
    };
    let result: xylem_protocols::ParseResult<u64> =
        xylem_core::threading::ParseResult::Retry(retry);
    let xylem_core::threading::ParseResult::Retry(retry) = result else {
        panic!("expected retry");
    };
    assert_eq!(retry.original_request_id, 7);
    assert_eq!(retry.target_conn_id, Some(1));
}
