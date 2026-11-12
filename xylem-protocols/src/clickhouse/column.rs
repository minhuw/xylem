//! Skipping column data of Native blocks without decoding it
//!
//! A block's column carries its type name, such as
//! `Array(LowCardinality(Nullable(String)))`. [`Types`] parses each name
//! once into a tree of the layouts below; [`Skipper`] then walks a column's
//! bytes by that tree, from any split of the bytes into reads. The layouts
//! follow ClickHouse's `NativeWriter` with the default serializations
//! (revisions before 54454 get no sparse or other custom ones):
//!
//! - fixed-width values (numbers, dates, decimals, UUIDs, ...): rows times
//!   the width
//! - `String`: per row a varint length and the bytes
//! - `Nullable(T)`: a byte per row, then the `T` column
//! - `Array(T)`: a cumulative u64 offset per row, then the `T` column with as
//!   many rows as the last offset; `Map(K, V)` is `Array(Tuple(K, V))`
//! - `Tuple(T1, ...)`: the columns of each element in turn; `Tuple()` is a
//!   byte per row
//! - `LowCardinality(T)`: granules until all rows are read, each a u64
//!   index type, a u64 key count and the keys (`T` without `Nullable`), a
//!   u64 row count and an index of 1, 2, 4 or 8 bytes per row
//!
//! Before its data, a column with rows has a u64 key version per
//! `LowCardinality` in its type. A column of a block without rows has no
//! data at all.

use anyhow::{bail, ensure, Context, Result};
use xylem_common::FastHashMap;

/// Most array elements of a column in one block, as ClickHouse's
/// `MAX_ARRAYS_SIZE`
const MAX_ELEMENTS: u64 = 1 << 40;

/// Index of a [`Layout`] in [`Types`]
type LayoutId = u32;

/// How a column's values are laid out
#[derive(Debug, Clone, PartialEq, Eq)]
enum Layout {
    /// This many bytes per row
    Fixed(u64),
    String,
    Nullable(LayoutId),
    Array(LayoutId),
    Tuple(Vec<LayoutId>),
    /// The layout of the dictionary keys
    LowCardinality(LayoutId),
}

/// A column type ready to skip
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnType {
    root: LayoutId,
    /// Bytes of `LowCardinality` key versions before the data
    prefix: u64,
}

/// The parsed column types seen so far, by name
#[derive(Debug, Default)]
pub struct Types {
    layouts: Vec<Layout>,
    by_name: FastHashMap<Box<[u8]>, ColumnType>,
}

impl Types {
    /// The column type named `name`, parsed the first time it is seen
    pub fn get(&mut self, name: &[u8]) -> Result<ColumnType> {
        if let Some(&column) = self.by_name.get(name) {
            return Ok(column);
        }
        let text = std::str::from_utf8(name).context("ClickHouse column type is not UTF-8")?;
        let mut low_cardinality = 0;
        let root = self
            .parse(text, &mut low_cardinality)
            .with_context(|| format!("ClickHouse column type {text}"))?;
        let column = ColumnType { root, prefix: 8 * low_cardinality };
        self.by_name.insert(name.into(), column);
        Ok(column)
    }

    fn add(&mut self, layout: Layout) -> LayoutId {
        self.layouts.push(layout);
        (self.layouts.len() - 1) as LayoutId
    }

    fn layout(&self, id: LayoutId) -> &Layout {
        &self.layouts[id as usize]
    }

    /// Parse a type name, counting its `LowCardinality`s
    fn parse(&mut self, text: &str, low_cardinality: &mut u64) -> Result<LayoutId> {
        let (name, args) = split_type(text)?;
        let args = match args {
            Some(args) => split_args(args)?,
            None => Vec::new(),
        };
        let arity = |n: usize| -> Result<()> {
            ensure!(args.len() == n, "{name} takes {n} arguments, not {}", args.len());
            Ok(())
        };
        if let Some(width) = fixed_width(name, &args)? {
            return Ok(self.add(Layout::Fixed(width)));
        }
        let layout = match name {
            "String" => {
                arity(0)?;
                Layout::String
            }
            "FixedString" => {
                arity(1)?;
                let width: u64 = args[0].parse().context("FixedString length")?;
                ensure!(width > 0, "FixedString length is 0");
                Layout::Fixed(width)
            }
            "Nullable" => {
                arity(1)?;
                Layout::Nullable(self.parse(args[0], low_cardinality)?)
            }
            "Array" => {
                arity(1)?;
                Layout::Array(self.parse(args[0], low_cardinality)?)
            }
            "Tuple" | "Nested" if args.is_empty() => Layout::Fixed(1),
            "Tuple" => Layout::Tuple(self.parse_elements(&args, low_cardinality)?),
            "Nested" => {
                let tuple = Layout::Tuple(self.parse_elements(&args, low_cardinality)?);
                Layout::Array(self.add(tuple))
            }
            "Map" => {
                arity(2)?;
                let elements = vec![
                    self.parse(args[0], low_cardinality)?,
                    self.parse(args[1], low_cardinality)?,
                ];
                Layout::Array(self.add(Layout::Tuple(elements)))
            }
            "LowCardinality" => {
                arity(1)?;
                *low_cardinality += 1;
                let (inner, inner_args) = split_type(args[0])?;
                let keys = match (inner, inner_args) {
                    ("Nullable", Some(keys)) => keys,
                    _ => args[0],
                };
                Layout::LowCardinality(self.parse(keys, low_cardinality)?)
            }
            "SimpleAggregateFunction" => {
                ensure!(args.len() >= 2, "SimpleAggregateFunction takes a function and a type");
                return self.parse(args[args.len() - 1], low_cardinality);
            }
            // Geo types are named tuples and arrays of points
            "Point" => Layout::Fixed(16),
            "Ring" | "LineString" => Layout::Array(self.parse("Point", low_cardinality)?),
            "Polygon" | "MultiLineString" => Layout::Array(self.parse("Ring", low_cardinality)?),
            "MultiPolygon" => Layout::Array(self.parse("Polygon", low_cardinality)?),
            _ => bail!(
                "xylem cannot skip {name} columns; it reads numbers, dates and times, decimals, \
                 enums, UUIDs, IP addresses, String, FixedString, Nullable, Array, Tuple, Map, \
                 Nested, LowCardinality, SimpleAggregateFunction and geo types"
            ),
        };
        Ok(self.add(layout))
    }

    /// Parse tuple elements, each a type or a name and a type
    fn parse_elements(
        &mut self,
        args: &[&str],
        low_cardinality: &mut u64,
    ) -> Result<Vec<LayoutId>> {
        args.iter()
            .map(|element| self.parse(element_type(element)?, low_cardinality))
            .collect()
    }
}

/// The width of a fixed-width type, `None` for other types
fn fixed_width(name: &str, args: &[&str]) -> Result<Option<u64>> {
    let width = match name {
        "UInt8" | "Int8" | "Bool" | "Enum8" | "Nothing" => 1,
        "UInt16" | "Int16" | "Date" | "Enum16" | "BFloat16" => 2,
        "UInt32" | "Int32" | "Float32" | "Date32" | "DateTime" | "IPv4" | "Time" | "Decimal32" => 4,
        "UInt64" | "Int64" | "Float64" | "DateTime64" | "Time64" | "Decimal64" => 8,
        "UInt128" | "Int128" | "UUID" | "IPv6" | "Decimal128" => 16,
        "UInt256" | "Int256" | "Decimal256" => 32,
        "Decimal" => {
            ensure!((1..=2).contains(&args.len()), "Decimal takes a precision and a scale");
            let precision: u32 = args[0].parse().context("Decimal precision")?;
            match precision {
                1..=9 => 4,
                10..=18 => 8,
                19..=38 => 16,
                39..=76 => 32,
                _ => bail!("Decimal precision {precision} is not in 1..=76"),
            }
        }
        // Int64 counts of an interval kind
        _ if name.starts_with("Interval") => 8,
        _ => return Ok(None),
    };
    Ok(Some(width))
}

/// Split `Name(args)` into the name and the text inside the parentheses
fn split_type(text: &str) -> Result<(&str, Option<&str>)> {
    let text = text.trim();
    let Some(open) = text.find('(') else {
        return Ok((text, None));
    };
    ensure!(text.ends_with(')'), "type {text} does not end with ')'");
    Ok((text[..open].trim_end(), Some(&text[open + 1..text.len() - 1])))
}

/// Split type arguments at the commas outside parentheses and quotes
fn split_args(args: &str) -> Result<Vec<&str>> {
    let bytes = args.as_bytes();
    let mut parts = Vec::new();
    let (mut depth, mut quote, mut start, mut i) = (0usize, None, 0, 0);
    while i < bytes.len() {
        let byte = bytes[i];
        match quote {
            Some(_) if byte == b'\\' => i += 1,
            Some(open) if byte == open => quote = None,
            Some(_) => {}
            None => match byte {
                b'\'' | b'"' | b'`' => quote = Some(byte),
                b'(' => depth += 1,
                b')' => depth = depth.checked_sub(1).context("unbalanced ')'")?,
                b',' if depth == 0 => {
                    parts.push(args[start..i].trim());
                    start = i + 1;
                }
                _ => {}
            },
        }
        i += 1;
    }
    ensure!(quote.is_none() && depth == 0, "unbalanced quotes or parentheses in ({args})");
    let last = args[start..].trim();
    if !parts.is_empty() || !last.is_empty() {
        parts.push(last);
    }
    ensure!(parts.iter().all(|part| !part.is_empty()), "empty type argument in ({args})");
    Ok(parts)
}

/// The type of a tuple element: `a UInt8`, `` `a b` UInt8 `` or `UInt8`
fn element_type(element: &str) -> Result<&str> {
    let element = element.trim();
    if let Some(quoted) = element.strip_prefix(['`', '"']) {
        let close = element.as_bytes()[0] as char;
        let end = quoted.find(close).context("unterminated element name")?;
        return Ok(quoted[end + 1..].trim());
    }
    // A bare type has no whitespace before its parenthesis, if any
    match element.find(|c: char| c.is_whitespace() || c == '(') {
        Some(end) if !element[end..].starts_with('(') => Ok(element[end..].trim()),
        _ => Ok(element),
    }
}

/// A step of skipping column data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Task {
    /// Skip this many bytes
    Bytes(u64),
    /// Skip this many strings
    Strings(u64),
    /// Skip a column of a layout with this many rows
    Column(LayoutId, u64),
    /// Read an array column's last offset, then skip that many elements
    LastOffset(LayoutId),
    /// Read a LowCardinality granule's index type and keys, of `rows`
    /// rows still to read
    Granule { keys: LayoutId, rows: u64 },
    /// Read a granule's row count and skip its index
    Index { keys: LayoutId, width: u64, rows: u64 },
}

/// Skips the data of one column; see the module documentation
#[derive(Debug, Default)]
pub struct Skipper {
    /// Pending steps, the next one last
    tasks: Vec<Task>,
}

impl Skipper {
    /// Start skipping a column of type `column` with `rows` rows
    pub fn start(&mut self, column: ColumnType, rows: u64) {
        debug_assert!(self.tasks.is_empty(), "the previous column was skipped");
        if rows > 0 {
            self.tasks.push(Task::Column(column.root, rows));
            self.tasks.push(Task::Bytes(column.prefix));
        }
    }

    /// Skip column bytes at the start of `data`, at most `budget` of them;
    /// returns how many were skipped and whether the column ended
    ///
    /// Parts that must be read whole (lengths, offsets, granule headers) are
    /// left for the next call when `data` holds only some of their bytes.
    pub fn skip(&mut self, types: &Types, data: &[u8], budget: usize) -> Result<(usize, bool)> {
        let data = &data[..data.len().min(budget)];
        let mut pos = 0;
        while let Some(task) = self.tasks.pop() {
            let rest = &data[pos..];
            match task {
                Task::Bytes(n) => {
                    let skipped = n.min(rest.len() as u64);
                    pos += skipped as usize;
                    if skipped < n {
                        self.tasks.push(Task::Bytes(n - skipped));
                        break;
                    }
                }
                Task::Strings(rows) => {
                    let (skipped, rows, partial) = skip_strings(rest, rows)?;
                    pos += skipped;
                    if rows > 0 {
                        self.tasks.push(Task::Strings(rows));
                    }
                    if let Some(left) = partial {
                        self.tasks.push(Task::Bytes(left));
                    }
                    if rows > 0 || partial.is_some() {
                        break;
                    }
                }
                Task::Column(layout, rows) => self.expand(types, layout, rows)?,
                Task::LastOffset(elements) => {
                    let Some(bytes) = rest.get(..8) else {
                        self.tasks.push(task);
                        break;
                    };
                    let count = u64::from_le_bytes(bytes.try_into().unwrap());
                    ensure!(
                        count <= MAX_ELEMENTS,
                        "ClickHouse array column has {count} elements, more than {MAX_ELEMENTS}"
                    );
                    pos += 8;
                    if count > 0 {
                        self.tasks.push(Task::Column(elements, count));
                    }
                }
                Task::Granule { keys, rows } => {
                    let Some(bytes) = rest.get(..16) else {
                        self.tasks.push(task);
                        break;
                    };
                    let index = u64::from_le_bytes(bytes[..8].try_into().unwrap());
                    let count = u64::from_le_bytes(bytes[8..].try_into().unwrap());
                    let width = granule_index_width(index)?;
                    pos += 16;
                    self.tasks.push(Task::Index { keys, width, rows });
                    if count > 0 {
                        self.tasks.push(Task::Column(keys, count));
                    }
                }
                Task::Index { keys, width, rows } => {
                    let Some(bytes) = rest.get(..8) else {
                        self.tasks.push(task);
                        break;
                    };
                    let count = u64::from_le_bytes(bytes.try_into().unwrap());
                    ensure!(
                        (1..=rows).contains(&count),
                        "ClickHouse LowCardinality granule has {count} rows; {rows} are left"
                    );
                    pos += 8;
                    if count < rows {
                        self.tasks.push(Task::Granule { keys, rows: rows - count });
                    }
                    self.tasks.push(Task::Bytes(count * width));
                }
            }
        }
        Ok((pos, self.tasks.is_empty()))
    }

    /// Replace a column of `rows` rows (at least one) of `layout` by the
    /// steps that skip it
    fn expand(&mut self, types: &Types, layout: LayoutId, rows: u64) -> Result<()> {
        let bytes = |rows: u64, width: u64| {
            rows.checked_mul(width)
                .map(Task::Bytes)
                .with_context(|| format!("ClickHouse column of {rows} rows is too long"))
        };
        match types.layout(layout) {
            &Layout::Fixed(width) => self.tasks.push(bytes(rows, width)?),
            Layout::String => self.tasks.push(Task::Strings(rows)),
            &Layout::Nullable(inner) => {
                self.tasks.push(Task::Column(inner, rows));
                self.tasks.push(Task::Bytes(rows));
            }
            &Layout::Array(elements) => {
                // The offsets but the last, then the last
                self.tasks.push(Task::LastOffset(elements));
                self.tasks.push(bytes(rows - 1, 8)?);
            }
            Layout::Tuple(elements) => self
                .tasks
                .extend(elements.iter().rev().map(|&element| Task::Column(element, rows))),
            &Layout::LowCardinality(keys) => self.tasks.push(Task::Granule { keys, rows }),
        }
        Ok(())
    }
}

/// Skip up to `rows` strings at the start of `data`: the bytes skipped, the
/// strings left, and the bytes left of a string cut short after its length
fn skip_strings(data: &[u8], mut rows: u64) -> Result<(usize, u64, Option<u64>)> {
    let mut at = 0;
    while rows > 0 {
        let Some((len, header)) = super::wire::varuint(&data[at..])? else {
            break;
        };
        rows -= 1;
        at += header;
        let available = (data.len() - at) as u64;
        if len > available {
            return Ok((data.len(), rows, Some(len - available)));
        }
        at += len as usize;
    }
    Ok((at, rows, None))
}

/// The bytes per row of a LowCardinality granule's index, from its index
/// type; Native blocks carry their keys in every granule
fn granule_index_width(index: u64) -> Result<u64> {
    const NEED_GLOBAL_DICTIONARY: u64 = 1 << 8;
    const HAS_ADDITIONAL_KEYS: u64 = 1 << 9;
    const NEED_UPDATE_DICTIONARY: u64 = 1 << 10;
    let flags = NEED_GLOBAL_DICTIONARY | HAS_ADDITIONAL_KEYS | NEED_UPDATE_DICTIONARY;
    ensure!(
        index & NEED_GLOBAL_DICTIONARY == 0 && index & HAS_ADDITIONAL_KEYS != 0,
        "ClickHouse LowCardinality granule has index type {index:#x}, which a Native block \
         cannot have (no keys of its own, or a global dictionary)"
    );
    match index & !flags {
        kind @ 0..=3 => Ok(1 << kind),
        kind => bail!("ClickHouse LowCardinality granule has index kind {kind}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A layout tree as text: fixed widths as numbers
    fn describe(types: &Types, id: LayoutId) -> String {
        match types.layout(id) {
            Layout::Fixed(width) => width.to_string(),
            Layout::String => "String".to_string(),
            &Layout::Nullable(inner) => format!("Nullable({})", describe(types, inner)),
            &Layout::Array(inner) => format!("Array({})", describe(types, inner)),
            &Layout::LowCardinality(keys) => format!("LowCardinality({})", describe(types, keys)),
            Layout::Tuple(elements) => {
                let elements: Vec<_> = elements.iter().map(|&e| describe(types, e)).collect();
                format!("Tuple({})", elements.join(", "))
            }
        }
    }

    fn layout_of(types: &mut Types, name: &str) -> Layout {
        let column = types.get(name.as_bytes()).unwrap();
        types.layout(column.root).clone()
    }

    #[test]
    fn type_names_parse_to_layouts() {
        let mut types = Types::default();
        for (name, width) in [
            ("UInt8", 1),
            ("Bool", 1),
            ("Enum8('a, b' = 1, 'c)' = 2)", 1),
            ("Enum16('x' = 1000)", 2),
            ("Date", 2),
            ("Int32", 4),
            ("DateTime", 4),
            ("DateTime('Europe/Amsterdam')", 4),
            ("Float64", 8),
            ("DateTime64(3, 'UTC')", 8),
            ("IntervalSecond", 8),
            ("Decimal(9, 2)", 4),
            ("Decimal(18, 4)", 8),
            ("Decimal(38, 10)", 16),
            ("Decimal(76, 0)", 32),
            ("Decimal128(3)", 16),
            ("UUID", 16),
            ("IPv6", 16),
            ("Int256", 32),
            ("FixedString(3)", 3),
            ("Point", 16),
            ("Tuple()", 1),
        ] {
            assert_eq!(layout_of(&mut types, name), Layout::Fixed(width), "{name}");
        }
        assert_eq!(layout_of(&mut types, "String"), Layout::String);
        let Layout::Tuple(elements) =
            layout_of(&mut types, "Tuple(a Nullable(String), `b c` DateTime64(6, 'UTC'), UInt8)")
        else {
            panic!("not a tuple");
        };
        let element = |i: usize| types.layout(elements[i]).clone();
        assert!(matches!(element(0), Layout::Nullable(_)));
        assert_eq!((element(1), element(2)), (Layout::Fixed(8), Layout::Fixed(1)));
        let Layout::Array(pairs) = layout_of(&mut types, "Map(LowCardinality(String), UInt64)")
        else {
            panic!("not an array");
        };
        assert!(matches!(types.layout(pairs), Layout::Tuple(elements) if elements.len() == 2));
        // LowCardinality keys drop Nullable; each LowCardinality has a key version
        let column = types
            .get(b"Array(Tuple(LowCardinality(Nullable(String)), LowCardinality(UInt8)))")
            .unwrap();
        assert_eq!(column.prefix, 16);
        let Layout::LowCardinality(keys) =
            layout_of(&mut types, "LowCardinality(Nullable(String))")
        else {
            panic!("not LowCardinality");
        };
        assert_eq!(types.layout(keys), &Layout::String);
        for (name, shape) in [
            (
                "SimpleAggregateFunction(sumMap, Tuple(Array(UInt8), Array(UInt64)))",
                "Tuple(Array(1), Array(8))",
            ),
            ("Nested(a UInt8, b String)", "Array(Tuple(1, String))"),
            ("MultiPolygon", "Array(Array(Array(16)))"),
            ("Map(String, Array(UInt16))", "Array(Tuple(String, Array(2)))"),
        ] {
            let column = types.get(name.as_bytes()).unwrap();
            assert_eq!(describe(&types, column.root), shape, "{name}");
        }
    }

    #[test]
    fn unsupported_and_malformed_types_fail() {
        let mut types = Types::default();
        for (name, error) in [
            ("Dynamic", "cannot skip Dynamic columns"),
            ("Variant(String, UInt64)", "cannot skip Variant columns"),
            ("AggregateFunction(uniq, UInt64)", "cannot skip AggregateFunction"),
            ("Array(JSON)", "cannot skip JSON"),
            ("Array(UInt8", "does not end with ')'"),
            ("Nullable(UInt8, UInt16)", "Nullable takes 1 arguments, not 2"),
            ("Decimal(80, 2)", "precision 80"),
            ("FixedString(x)", "FixedString length"),
            ("Enum8('a = 1)", "unbalanced quotes"),
        ] {
            let message = format!("{:#}", types.get(name.as_bytes()).unwrap_err());
            assert!(message.contains(error), "{name}: {message}");
        }
    }
}
