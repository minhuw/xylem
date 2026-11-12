-- A small table of items: 100,000 rows of common column types
CREATE TABLE bench.items
(
    id UInt64,
    name String,
    category LowCardinality(String),
    price Decimal(10, 2),
    tags Array(String),
    created DateTime('UTC'),
    note Nullable(String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO bench.items
SELECT
    number,
    concat('item-', toString(number)),
    ['books', 'games', 'music', 'tools'][number % 4 + 1],
    toDecimal64(number % 1000, 2) / 10,
    arrayMap(i -> concat('tag', toString(i)), range(number % 4)),
    toDateTime('2026-01-01 00:00:00', 'UTC') + number,
    if(number % 3 = 0, NULL, concat('note ', toString(number)))
FROM numbers(100000);
