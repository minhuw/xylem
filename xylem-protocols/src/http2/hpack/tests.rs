use super::*;

/// A header block's hex dump, the dynamic table size after decoding it, and
/// the fields it decodes to
type Example = (&'static str, usize, &'static [(&'static str, &'static str)]);

/// RFC 7541 C.3 (requests) and C.4 (the same with Huffman coding), decoded
/// in turn with the default table size
const REQUESTS: [[Example; 3]; 2] = [
    [
        (
            "828684410f7777772e6578616d706c652e636f6d",
            57,
            &[
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                (":authority", "www.example.com"),
            ],
        ),
        (
            "828684be58086e6f2d6361636865",
            110,
            &[
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                (":authority", "www.example.com"),
                ("cache-control", "no-cache"),
            ],
        ),
        (
            "828785bf400a637573746f6d2d6b65790c637573746f6d2d76616c7565",
            164,
            &[
                (":method", "GET"),
                (":scheme", "https"),
                (":path", "/index.html"),
                (":authority", "www.example.com"),
                ("custom-key", "custom-value"),
            ],
        ),
    ],
    [
        (
            "828684418cf1e3c2e5f23a6ba0ab90f4ff",
            57,
            &[
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                (":authority", "www.example.com"),
            ],
        ),
        (
            "828684be5886a8eb10649cbf",
            110,
            &[
                (":method", "GET"),
                (":scheme", "http"),
                (":path", "/"),
                (":authority", "www.example.com"),
                ("cache-control", "no-cache"),
            ],
        ),
        (
            "828785bf408825a849e95ba97d7f8925a849e95bb8e8b4bf",
            164,
            &[
                (":method", "GET"),
                (":scheme", "https"),
                (":path", "/index.html"),
                (":authority", "www.example.com"),
                ("custom-key", "custom-value"),
            ],
        ),
    ],
];

/// RFC 7541 C.5 (responses) and C.6 (the same with Huffman coding), decoded
/// in turn with a 256-byte table, so entries are evicted
const RESPONSES: [[Example; 3]; 2] = [
    [
        ("4803333032580770726976617465611d4d6f6e2c203231204f637420323031332032303a31333a323120474d546e1768747470733a2f2f7777772e6578616d706c652e636f6d", 222, &[(":status", "302"), ("cache-control", "private"), ("date", "Mon, 21 Oct 2013 20:13:21 GMT"), ("location", "https://www.example.com")]),
        ("4803333037c1c0bf", 222, &[(":status", "307"), ("cache-control", "private"), ("date", "Mon, 21 Oct 2013 20:13:21 GMT"), ("location", "https://www.example.com")]),
        ("88c1611d4d6f6e2c203231204f637420323031332032303a31333a323220474d54c05a04677a69707738666f6f3d4153444a4b48514b425a584f5157454f50495541585157454f49553b206d61782d6167653d333630303b2076657273696f6e3d31", 215, &[(":status", "200"), ("cache-control", "private"), ("date", "Mon, 21 Oct 2013 20:13:22 GMT"), ("location", "https://www.example.com"), ("content-encoding", "gzip"), ("set-cookie", "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1")]),
    ],
    [
        ("488264025885aec3771a4b6196d07abe941054d444a8200595040b8166e082a62d1bff6e919d29ad171863c78f0b97c8e9ae82ae43d3", 222, &[(":status", "302"), ("cache-control", "private"), ("date", "Mon, 21 Oct 2013 20:13:21 GMT"), ("location", "https://www.example.com")]),
        ("4883640effc1c0bf", 222, &[(":status", "307"), ("cache-control", "private"), ("date", "Mon, 21 Oct 2013 20:13:21 GMT"), ("location", "https://www.example.com")]),
        ("88c16196d07abe941054d444a8200595040b8166e084a62d1bffc05a839bd9ab77ad94e7821dd7f2e6c7b335dfdfcd5b3960d5af27087f3672c1ab270fb5291f9587316065c003ed4ee5b1063d5007", 215, &[(":status", "200"), ("cache-control", "private"), ("date", "Mon, 21 Oct 2013 20:13:22 GMT"), ("location", "https://www.example.com"), ("content-encoding", "gzip"), ("set-cookie", "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1")]),
    ],
];

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

/// Decode `block` into owned name and value strings
fn fields(decoder: &mut Decoder, block: &[u8]) -> Result<Vec<(String, String)>> {
    let mut fields = Vec::new();
    decoder.decode(block, |name, value| {
        let text = |bytes: &[u8]| String::from_utf8(bytes.to_vec()).unwrap();
        fields.push((text(name), text(value)));
        Ok(())
    })?;
    Ok(fields)
}

fn check_sequence(decoder: &mut Decoder, examples: &[Example]) {
    for (dump, table_size, expected) in examples {
        let decoded = fields(decoder, &hex(dump)).unwrap();
        let expected: Vec<_> =
            expected.iter().map(|&(n, v)| (n.to_string(), v.to_string())).collect();
        assert_eq!(decoded, expected, "{dump}");
        assert_eq!(decoder.table_size(), *table_size, "{dump}");
    }
}

#[test]
fn decodes_rfc_7541_request_examples() {
    for examples in &REQUESTS {
        check_sequence(&mut Decoder::default(), examples);
    }
}

#[test]
fn decodes_rfc_7541_response_examples_with_evictions() {
    for examples in &RESPONSES {
        check_sequence(&mut Decoder::new(256), examples);
    }
}

#[test]
fn decodes_rfc_7541_field_representations() {
    // C.2.1 to C.2.4, each on a fresh decoder
    let cases: [Example; 4] = [
        (
            "400a637573746f6d2d6b65790d637573746f6d2d686561646572",
            55,
            &[("custom-key", "custom-header")],
        ),
        ("040c2f73616d706c652f70617468", 0, &[(":path", "/sample/path")]),
        ("100870617373776f726406736563726574", 0, &[("password", "secret")]),
        ("82", 0, &[(":method", "GET")]),
    ];
    for case in cases {
        check_sequence(&mut Decoder::default(), &[case]);
    }
}

#[test]
fn integers_use_their_prefix_and_continuation_bytes() {
    // RFC 7541 C.1: 10 and 1337 in 5-bit prefixes, 42 at an octet boundary
    for (value, prefix, wire) in
        [(10, 5, &[0x0a][..]), (1337, 5, &[0x1f, 0x9a, 0x0a]), (42, 8, &[0x2a])]
    {
        let mut out = Vec::new();
        encode_int(&mut out, 0, prefix, value);
        assert_eq!(out, wire);
        let mut pos = 0;
        assert_eq!(decode_int(wire, &mut pos, prefix).unwrap(), value);
        assert_eq!(pos, wire.len());
    }
    let mut pos = 0;
    assert!(decode_int(&[0x1f, 0x9a], &mut pos, 5).is_err());
    let mut pos = 0;
    assert!(decode_int(&[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01], &mut pos, 7).is_err());
}

#[test]
fn size_updates_evict_and_are_bounded() {
    let mut decoder = Decoder::default();
    fields(&mut decoder, &hex("400a637573746f6d2d6b65790d637573746f6d2d686561646572")).unwrap();
    assert_eq!(decoder.table_size(), 55);
    // A size update to 0 empties the table, and the dynamic index is gone
    fields(&mut decoder, &[0x20]).unwrap();
    assert_eq!(decoder.table_size(), 0);
    assert!(fields(&mut decoder, &[0xbe]).is_err());
    // Above the advertised limit
    assert!(fields(&mut Decoder::default(), &[0x3f, 0xe2, 0x1f]).is_err());
    // Only at the start of a block
    assert!(fields(&mut Decoder::default(), &[0x82, 0x20]).is_err());
    // An entry larger than the table empties it and is not added
    let mut decoder = Decoder::new(40);
    fields(&mut decoder, &hex("4003616263016b")).unwrap();
    assert_eq!(decoder.table_size(), 36);
    fields(&mut decoder, &hex("400a637573746f6d2d6b65790d637573746f6d2d686561646572")).unwrap();
    assert_eq!(decoder.table_size(), 0);
}

#[test]
fn malformed_blocks_are_errors() {
    for block in [
        &[0x80][..],         // index 0
        &[0xff, 0x00],       // past the dynamic table
        &[0x40, 0x05, b'a'], // name cut short
        &[0x04],             // value missing
        &[0x04, 0x85, 0xff], // value cut short
        &[0x04, 0x81, 0x00], // Huffman padding of zeros
    ] {
        assert!(fields(&mut Decoder::default(), block).is_err(), "{block:02x?}");
    }
}

#[test]
fn encoder_indexes_static_fields_and_never_adds_entries() {
    let mut block = Vec::new();
    encode_field(&mut block, ":method", "GET");
    encode_field(&mut block, ":scheme", "http");
    encode_field(&mut block, ":path", "/");
    encode_field(&mut block, ":method", "PUT");
    encode_field(&mut block, ":authority", "www.example.com");
    encode_field(&mut block, "te", "trailers");
    let mut expected = vec![0x82, 0x86, 0x84, 0x02, 0x03];
    expected.extend_from_slice(b"PUT");
    expected.extend_from_slice(&[0x01, 0x0f]);
    expected.extend_from_slice(b"www.example.com");
    expected.extend_from_slice(&[0x00, 0x02]);
    expected.extend_from_slice(b"te");
    expected.push(0x08);
    expected.extend_from_slice(b"trailers");
    assert_eq!(block, expected);
    let mut decoder = Decoder::default();
    let decoded = fields(&mut decoder, &block).unwrap();
    assert_eq!(decoded[3], (":method".to_string(), "PUT".to_string()));
    assert_eq!(decoded[5], ("te".to_string(), "trailers".to_string()));
    assert_eq!(decoder.table_size(), 0);
    // A long value takes a multi-byte length
    let long = "x".repeat(300);
    block.clear();
    encode_field(&mut block, "x-long", &long);
    assert_eq!(&block[..9], &[0x00, 0x06, b'x', b'-', b'l', b'o', b'n', b'g', 0x7f]);
    assert_eq!(fields(&mut decoder, &block).unwrap()[0].1, long);
}
