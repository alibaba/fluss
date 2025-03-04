---
sidebar_label: Versioned
sidebar_position: 3
---

# Versioned Merge Engine

By setting `'table.merge-engine' = 'versioned'`, users can update data based on the configured version column. Updates will be carried out when the latest value of the specified field is greater than or equal to the stored value. If it is less than or null, no update will be made.
This feature is particularly valuable for replacing [Deduplication](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/queries/deduplication/) transformations in streaming computations, reducing complexity and improving overall efficiency.

:::note
When using `versioned` merge engine, there are the following limits:
- `UPDATE` and `DELETE` statements are not supported.
- Partial update is not supported.
- `UPDATE_BEFORE` and `DELETE` changelog events are ignored automatically.
:::

## Versioned Merge Column

:::note
The versioned merge column supports the following data types.
- INT
- BIGINT
- TIMESTAMP
- TIMESTAMP(p)
- TIMESTAMP_LTZ
- TIMESTAMP_LTZ(p)
:::

example:
```sql title="Flink SQL"

CREATE TABLE VERSIONED (
    a INT NOT NULL PRIMARY KEY NOT ENFORCED,
    b STRING, 
    ts BIGINT
 ) WITH (
    'table.merge-engine' = 'versioned',
    'table.merge-engine.versioned.ver-column' = 'ts'
);
INSERT INTO VERSIONED (a, b, ts) VALUES (1, 'v1', 1000);

-- insert data with ts < 1000, no update will be made
INSERT INTO VERSIONED (a, b, ts) VALUES (1, 'v2', 999);
SELECT * FROM VERSIONED;
-- Output
-- +---+-----+------+
-- | a | b   | ts   |
-- +---+-----+------+
-- | 1 | v1  | 1000 |
-- +---+-----+------+


-- insert data with ts > 1000, update will be made
INSERT INTO VERSIONED (a, b, ts) VALUES (1, 'v2', 2000);
SELECT * FROM VERSIONED;
-- Output
-- +---+-----+------+
-- | a | b   | ts   |
-- +---+-----+------+
-- | 1 | v2  | 2000 |
-- +---+-----+------+

-- insert data with ts = null, no update will be made
INSERT INTO VERSIONED (a, b, ts) VALUES (1, 'v2', null);
SELECT * FROM VERSIONED;
-- Output
-- +---+-----+------+
-- | a | b   | ts   |
-- +---+-----+------+
-- | 1 | v2  | 2000 |
-- +---+-----+------+

```
