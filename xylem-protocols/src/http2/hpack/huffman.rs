//! HPACK's Huffman code (RFC 7541 section 5.2 and Appendix B), decoding only

use anyhow::{bail, Result};
use std::sync::OnceLock;

/// The EOS symbol, which must never be decoded
const EOS: u16 = 256;

/// Marks a tree node's child as a leaf holding the symbol in the low bits
const LEAF: u16 = 0x8000;

/// The code and its length in bits of each symbol, EOS last
const CODES: [(u32, u8); 257] = [
    (0x1ff8, 13),
    (0x7fffd8, 23),
    (0xfffffe2, 28),
    (0xfffffe3, 28),
    (0xfffffe4, 28),
    (0xfffffe5, 28),
    (0xfffffe6, 28),
    (0xfffffe7, 28),
    (0xfffffe8, 28),
    (0xffffea, 24),
    (0x3ffffffc, 30),
    (0xfffffe9, 28),
    (0xfffffea, 28),
    (0x3ffffffd, 30),
    (0xfffffeb, 28),
    (0xfffffec, 28),
    (0xfffffed, 28),
    (0xfffffee, 28),
    (0xfffffef, 28),
    (0xffffff0, 28),
    (0xffffff1, 28),
    (0xffffff2, 28),
    (0x3ffffffe, 30),
    (0xffffff3, 28),
    (0xffffff4, 28),
    (0xffffff5, 28),
    (0xffffff6, 28),
    (0xffffff7, 28),
    (0xffffff8, 28),
    (0xffffff9, 28),
    (0xffffffa, 28),
    (0xffffffb, 28),
    (0x14, 6),
    (0x3f8, 10),
    (0x3f9, 10),
    (0xffa, 12),
    (0x1ff9, 13),
    (0x15, 6),
    (0xf8, 8),
    (0x7fa, 11),
    (0x3fa, 10),
    (0x3fb, 10),
    (0xf9, 8),
    (0x7fb, 11),
    (0xfa, 8),
    (0x16, 6),
    (0x17, 6),
    (0x18, 6),
    (0x0, 5),
    (0x1, 5),
    (0x2, 5),
    (0x19, 6),
    (0x1a, 6),
    (0x1b, 6),
    (0x1c, 6),
    (0x1d, 6),
    (0x1e, 6),
    (0x1f, 6),
    (0x5c, 7),
    (0xfb, 8),
    (0x7ffc, 15),
    (0x20, 6),
    (0xffb, 12),
    (0x3fc, 10),
    (0x1ffa, 13),
    (0x21, 6),
    (0x5d, 7),
    (0x5e, 7),
    (0x5f, 7),
    (0x60, 7),
    (0x61, 7),
    (0x62, 7),
    (0x63, 7),
    (0x64, 7),
    (0x65, 7),
    (0x66, 7),
    (0x67, 7),
    (0x68, 7),
    (0x69, 7),
    (0x6a, 7),
    (0x6b, 7),
    (0x6c, 7),
    (0x6d, 7),
    (0x6e, 7),
    (0x6f, 7),
    (0x70, 7),
    (0x71, 7),
    (0x72, 7),
    (0xfc, 8),
    (0x73, 7),
    (0xfd, 8),
    (0x1ffb, 13),
    (0x7fff0, 19),
    (0x1ffc, 13),
    (0x3ffc, 14),
    (0x22, 6),
    (0x7ffd, 15),
    (0x3, 5),
    (0x23, 6),
    (0x4, 5),
    (0x24, 6),
    (0x5, 5),
    (0x25, 6),
    (0x26, 6),
    (0x27, 6),
    (0x6, 5),
    (0x74, 7),
    (0x75, 7),
    (0x28, 6),
    (0x29, 6),
    (0x2a, 6),
    (0x7, 5),
    (0x2b, 6),
    (0x76, 7),
    (0x2c, 6),
    (0x8, 5),
    (0x9, 5),
    (0x2d, 6),
    (0x77, 7),
    (0x78, 7),
    (0x79, 7),
    (0x7a, 7),
    (0x7b, 7),
    (0x7ffe, 15),
    (0x7fc, 11),
    (0x3ffd, 14),
    (0x1ffd, 13),
    (0xffffffc, 28),
    (0xfffe6, 20),
    (0x3fffd2, 22),
    (0xfffe7, 20),
    (0xfffe8, 20),
    (0x3fffd3, 22),
    (0x3fffd4, 22),
    (0x3fffd5, 22),
    (0x7fffd9, 23),
    (0x3fffd6, 22),
    (0x7fffda, 23),
    (0x7fffdb, 23),
    (0x7fffdc, 23),
    (0x7fffdd, 23),
    (0x7fffde, 23),
    (0xffffeb, 24),
    (0x7fffdf, 23),
    (0xffffec, 24),
    (0xffffed, 24),
    (0x3fffd7, 22),
    (0x7fffe0, 23),
    (0xffffee, 24),
    (0x7fffe1, 23),
    (0x7fffe2, 23),
    (0x7fffe3, 23),
    (0x7fffe4, 23),
    (0x1fffdc, 21),
    (0x3fffd8, 22),
    (0x7fffe5, 23),
    (0x3fffd9, 22),
    (0x7fffe6, 23),
    (0x7fffe7, 23),
    (0xffffef, 24),
    (0x3fffda, 22),
    (0x1fffdd, 21),
    (0xfffe9, 20),
    (0x3fffdb, 22),
    (0x3fffdc, 22),
    (0x7fffe8, 23),
    (0x7fffe9, 23),
    (0x1fffde, 21),
    (0x7fffea, 23),
    (0x3fffdd, 22),
    (0x3fffde, 22),
    (0xfffff0, 24),
    (0x1fffdf, 21),
    (0x3fffdf, 22),
    (0x7fffeb, 23),
    (0x7fffec, 23),
    (0x1fffe0, 21),
    (0x1fffe1, 21),
    (0x3fffe0, 22),
    (0x1fffe2, 21),
    (0x7fffed, 23),
    (0x3fffe1, 22),
    (0x7fffee, 23),
    (0x7fffef, 23),
    (0xfffea, 20),
    (0x3fffe2, 22),
    (0x3fffe3, 22),
    (0x3fffe4, 22),
    (0x7ffff0, 23),
    (0x3fffe5, 22),
    (0x3fffe6, 22),
    (0x7ffff1, 23),
    (0x3ffffe0, 26),
    (0x3ffffe1, 26),
    (0xfffeb, 20),
    (0x7fff1, 19),
    (0x3fffe7, 22),
    (0x7ffff2, 23),
    (0x3fffe8, 22),
    (0x1ffffec, 25),
    (0x3ffffe2, 26),
    (0x3ffffe3, 26),
    (0x3ffffe4, 26),
    (0x7ffffde, 27),
    (0x7ffffdf, 27),
    (0x3ffffe5, 26),
    (0xfffff1, 24),
    (0x1ffffed, 25),
    (0x7fff2, 19),
    (0x1fffe3, 21),
    (0x3ffffe6, 26),
    (0x7ffffe0, 27),
    (0x7ffffe1, 27),
    (0x3ffffe7, 26),
    (0x7ffffe2, 27),
    (0xfffff2, 24),
    (0x1fffe4, 21),
    (0x1fffe5, 21),
    (0x3ffffe8, 26),
    (0x3ffffe9, 26),
    (0xffffffd, 28),
    (0x7ffffe3, 27),
    (0x7ffffe4, 27),
    (0x7ffffe5, 27),
    (0xfffec, 20),
    (0xfffff3, 24),
    (0xfffed, 20),
    (0x1fffe6, 21),
    (0x3fffe9, 22),
    (0x1fffe7, 21),
    (0x1fffe8, 21),
    (0x7ffff3, 23),
    (0x3fffea, 22),
    (0x3fffeb, 22),
    (0x1ffffee, 25),
    (0x1ffffef, 25),
    (0xfffff4, 24),
    (0xfffff5, 24),
    (0x3ffffea, 26),
    (0x7ffff4, 23),
    (0x3ffffeb, 26),
    (0x7ffffe6, 27),
    (0x3ffffec, 26),
    (0x3ffffed, 26),
    (0x7ffffe7, 27),
    (0x7ffffe8, 27),
    (0x7ffffe9, 27),
    (0x7ffffea, 27),
    (0x7ffffeb, 27),
    (0xffffffe, 28),
    (0x7ffffec, 27),
    (0x7ffffed, 27),
    (0x7ffffee, 27),
    (0x7ffffef, 27),
    (0x7fffff0, 27),
    (0x3ffffee, 26),
    (0x3fffffff, 30),
];

/// The decoding tree: node `n` has its children for a 0 and a 1 bit at
/// `[n][0]` and `[n][1]`, each another node or a [`LEAF`]
fn tree() -> &'static [[u16; 2]] {
    static TREE: OnceLock<Vec<[u16; 2]>> = OnceLock::new();
    TREE.get_or_init(|| {
        let mut nodes = vec![[0u16; 2]];
        for (symbol, &(code, len)) in CODES.iter().enumerate() {
            let mut node = 0;
            for bit in (0..len).rev() {
                let side = ((code >> bit) & 1) as usize;
                if bit == 0 {
                    nodes[node][side] = LEAF | symbol as u16;
                    break;
                }
                if nodes[node][side] == 0 {
                    nodes.push([0; 2]);
                    nodes[node][side] = (nodes.len() - 1) as u16;
                }
                node = usize::from(nodes[node][side]);
            }
        }
        nodes
    })
}

/// Append the Huffman decoding of `data` to `out`
///
/// The padding after the last symbol must be fewer than 8 bits, all ones
/// (a prefix of EOS); an encoded EOS is an error.
pub fn decode(data: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let tree = tree();
    let mut node = 0usize;
    // Bits read since the last symbol, and whether they were all ones
    let mut depth = 0u32;
    let mut ones = true;
    for &byte in data {
        for bit in (0..8).rev() {
            let side = usize::from((byte >> bit) & 1);
            depth += 1;
            ones &= side == 1;
            let next = tree[node][side];
            if next & LEAF != 0 {
                let symbol = next & !LEAF;
                if symbol == EOS {
                    bail!("HPACK Huffman string contains EOS");
                }
                out.push(symbol as u8);
                node = 0;
                depth = 0;
                ones = true;
            } else {
                node = usize::from(next);
            }
        }
    }
    if depth > 7 || !ones {
        bail!("HPACK Huffman string has invalid padding");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Huffman-encode `data` (for tests only: xylem sends raw strings)
    fn encode(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let (mut acc, mut bits) = (0u64, 0u32);
        for &byte in data {
            let (code, len) = CODES[usize::from(byte)];
            acc = acc << len | u64::from(code);
            bits += u32::from(len);
            while bits >= 8 {
                bits -= 8;
                out.push((acc >> bits) as u8);
            }
        }
        if bits > 0 {
            out.push((acc << (8 - bits)) as u8 | (0xff >> bits));
        }
        out
    }

    fn decoded(data: &[u8]) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        decode(data, &mut out).map(|()| out)
    }

    #[test]
    fn decodes_the_rfc_examples() {
        // RFC 7541 C.4.1 and C.6.1
        let www = [0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff];
        assert_eq!(decoded(&www).unwrap(), b"www.example.com");
        assert_eq!(decoded(&[0x64, 0x02]).unwrap(), b"302");
        assert_eq!(decoded(&[0xae, 0xc3, 0x77, 0x1a, 0x4b]).unwrap(), b"private");
        assert_eq!(decoded(&[]).unwrap(), b"");
    }

    #[test]
    fn every_byte_round_trips() {
        let all: Vec<u8> = (0..=255).collect();
        assert_eq!(decoded(&encode(&all)).unwrap(), all);
        for byte in 0..=255u8 {
            assert_eq!(decoded(&encode(&[byte, byte])).unwrap(), [byte, byte]);
        }
    }

    #[test]
    fn rejects_bad_padding_and_eos() {
        // '0' is 00000 (5 bits): padding of zeros is invalid
        assert!(decoded(&[0x00]).is_err());
        assert_eq!(decoded(&[0x07]).unwrap(), b"0");
        // A whole byte of padding is too long
        assert!(decoded(&[0x07, 0xff]).is_err());
        // EOS itself: 30 one bits
        assert!(decoded(&[0xff, 0xff, 0xff, 0xff]).is_err());
    }
}
