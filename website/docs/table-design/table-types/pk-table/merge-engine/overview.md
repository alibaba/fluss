---
sidebar_label: Overview
sidebar_position: 1
---

# Overview

When Fluss sink receives two or more rows with the same primary key for primary key table, it will merge them into
one row to keep primary key unique.
By default, it will only keep the latest row in the table. But by specifying the `table.merge-engine` table property,
users can choose how rows are merge into one row.

The following merge engines are supported:

1. [First Row](table-design/table-types/pk-table/merge-engine/first-row.md)