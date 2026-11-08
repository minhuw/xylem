//! Strict deserialization of profile sections

use serde::de::{self, Deserializer, MapAccess, Visitor};

/// Deserialize a unit variant of an internally tagged enum, rejecting every
/// key besides the tag. Use it as the variant's `deserialize_with`: serde
/// skips such keys even under `deny_unknown_fields`, so `type =
/// "closed-loop"` with `rate = 100` would otherwise load and drop the rate.
pub(crate) fn unit_variant<'de, D: Deserializer<'de>>(deserializer: D) -> Result<(), D::Error> {
    struct NoFields;

    impl<'de> Visitor<'de> for NoFields {
        type Value = ();

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("no fields besides the tag")
        }

        fn visit_unit<E: de::Error>(self) -> Result<(), E> {
            Ok(())
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
            match map.next_key::<String>()? {
                Some(key) => Err(de::Error::unknown_field(&key, &[])),
                None => Ok(()),
            }
        }
    }

    deserializer.deserialize_any(NoFields)
}
