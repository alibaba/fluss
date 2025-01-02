---
sidebar_label: First Row
sidebar_position: 2
---

# First Row

By specifying `'table.merge-engine' = 'first_row'`, users can keep the first row of the same primary key. It'll only
generate insert only change log, so that the downstream table of it can be append-only table.

:::note
When using `first_row` merge engine, there are the following limits:

- `UPDATE` and `DELETE` statements are not supported
- Partial update is not supported
  :::