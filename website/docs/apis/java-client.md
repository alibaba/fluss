---
sidebar_label: "Java Client"
sidebar_position: 1
---

<!--
 Copyright (c) 2025 Alibaba Group Holding Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Fluss  Java Client
## Overview
`Fluss Admin` is a thread-safe client that supports asynchronous operations for managing and inspecting Fluss resources. It communicates with the Fluss cluster via RPC calls and provides methods for:


* Managing databases (create, drop, list)
* Managing tables (create, drop, list)
* Managing partitions (create, drop, list)
* Retrieving metadata (schemas, snapshots, server information)

## Initialization

```java
Configuration conf = new Configuration(); 
conf.setString("bootstrap.servers", "localhost:9123");


Connection connection = ConnectionFactory.createConnection(conf);
Admin admin = connection.getAdmin();

admin.listDatabases().get().forEach(System.out::println);
```

## Working Operations
All methods in `FlussAdmin` return `CompletableFuture` objects. You can handle these in two ways:

### Blocking Operations
For synchronous behavior, use the `get()` method:
```java
// Blocking call
List<String> databases = admin.listDatabases().get();
```

### Asynchronous Operations
For non-blocking behavior, use the `thenAccept`, `thenApply`, or other methods:
```java
admin.listDatabases()
    .thenAccept(databases -> {
        System.out.println("Available databases:");
        databases.forEach(System.out::println);
    })
    .exceptionally(ex -> {
        System.err.println("Failed to list databases: " + ex.getMessage());
        return null;
    });
```

## Database Operations
### Creating a Database
```java

// Create database descriptor
DatabaseDescriptor descriptor = DatabaseDescriptor.builder()
    .comment("This is a test database")
    .customProperty("owner", "data-team")
    .build();

// Create database (true means ignore if exists)
admin.createDatabase("test_db", descriptor, true)
    .thenAccept(unused -> System.out.println("Database created successfully"))
    .exceptionally(ex -> {
        System.err.println("Failed to create database: " + ex.getMessage());
        return null;
    });
```

#### Example Output
```shell
Database created successfully
{"version":1,"comment":"This is a test database","custom_properties":{"owner":"data-team"}}
```
### List Databases
```java
List<String> databases = admin.listDatabases().get();
```

###  Drop a Database
```java
admin.dropDatabase("test_db", true, true).get();
```

### Database Info
```java
DatabaseInfo dbInfo = admin.getDatabaseInfo("test_db").get();
```

### Database Exists
```java
admin.databaseExists("test_db").get()
```

## Managing Tables
### Create a Table
```java
Schema schema = Schema.newBuilder()
        .column("id", DataTypes.STRING())
        .column("age", DataTypes.INT())
        .column("created_at", DataTypes.TIMESTAMP())
        .column("is_active", DataTypes.BOOLEAN())
        .primaryKey("id")
        .build();

// Use the schema in a table descriptor
TableDescriptor tableDescriptor = TableDescriptor.builder()
        .schema(schema)
        .distributedBy(1, "id")  // Distribute by the id column with 1 buckets
//        .partitionedBy("")     // Partition by the partition key
        .build();

TablePath tablePath = TablePath.of("fluss", "test");
admin.createTable(tablePath, tableDescriptor, false).get();
```

### Table Info
```java
TableInfo tableInfo = admin.getTableInfo(tablePath).get();
System.out.println(tableInfo);
```

#### Example Output
```shell
TableInfo{tablePath=fluss.test, tableId=3, schemaId=1, schema=(id STRING NOT NULL,age INT,created_at TIMESTAMP(6),is_active BOOLEAN,CONSTRAINT PK_id PRIMARY KEY (id)), physicalPrimaryKeys=[id], bucketKeys=[id], partitionKeys=[], numBuckets=1, properties={table.replication.factor=1}, customProperties={}, comment='null', createdTime=1744267377500, modifiedTime=1744267377500}
```
### List Tables in a Database
```java
List<String> tables = admin.listTables("test_db").get();
```

### Table Schema
```java
SchemaInfo schemaInfo = admin.getTableSchema(tablePath).get();
```

### Drop a Table
```java
admin.dropTable(TablePath.of("test_db", "test_table"), true).get();
```

### Table Exists
```java
admin.tableExists(tablePath).get();
```

## Managing Partitions
### Create Partition
```java
admin.createPartition(tablePath, new PartitionSpec(Collections.singletonMap("age", "10")).get();
```

### List Partitions
```java
List<PartitionInfo> partitions = admin.listPartitionInfos(TablePath.of("fluss", "test_table")).get();
partitions.forEach(System.out::println);
```

### Drop Partition
```java
admin.dropPartition(tablePath, new PartitionSpec(Collections.singletonMap("age", "10")).get();
```

## Retrieving Metadata
### Get Latest KV Snapshots
```java
KvSnapshots snapshots = admin.getLatestKvSnapshots(tablePath).get();
System.out.println(snapshots.getSnapshotId(1));
System.out.println(snapshots.getBucketIds());
System.out.println(snapshots.getLogOffset(1));
System.out.println(snapshots.getTableId());
System.out.println(snapshots.getPartitionId());
```

### Get KV Snapshot Metadata
```java
KvSnapshotMetadata snapshotMetadata = admin.getKvSnapshotMetadata(tableBucket, snapshotId.getAsLong()).get();
```

### Get Server Nodes
```java
List<ServerNode> serverNodes = admin.getServerNodes().get();
```


## Writers
In order to write data to Fluss tables, first you need to create a Table instance.
```java
TablePath tablePath = TablePath.of("fluss", "users");
Table table = connection.getTable(tablePath);
```

In Fluss we have both Primary Key tables and Log Tables, so the client provides different functionality depending on the table type.
You can use an `UpsertWriter` to write data to a Primary Key table, and an `Append` to write data to a Log Table.
````java
table.newUpsert().createWriter();
table.newAppend().createWriter();
````

Let's take a look at how to write data to a Primary Key table.
```java
List<User> users = List.of(
        new User("1", 20, LocalDateTime.now() , true),
        new User("2", 22, LocalDateTime.now() , true),
        new User("3", 23, LocalDateTime.now() , true),
        new User("4", 24, LocalDateTime.now() , true),
        new User("5", 25, LocalDateTime.now() , true)
);
```

**Note:** Currently data in Fluss is written in the form of `rows`, so we need to convert our POJO to `GenericRow`, while the Fluss community is working to provide
a more user-friendly API for writing data.
```java
Table table = connection.getTable(tablePath);

List<GenericRow> rows = users.stream().map(user -> {
    GenericRow row = new GenericRow(4);
    row.setField(0, BinaryString.fromString(user.getId()));
    row.setField(1, user.getAge());
    row.setField(2, TimestampNtz.fromLocalDateTime(user.getCreatedAt()));
    row.setField(3, user.isActive());
    return row;
}).collect(Collectors.toList());
        
System.out.println("Upserting rows to the table");
rows.forEach(row -> {     
    try {
        table.newUpsert().createWriter().upsert(row).get();
    } catch (Exception e) {
        e.printStackTrace();
    }
});
```

For a Log table you can use the `Append` API to write data.
```java
table.newAppend().createWriter().append(row).get();
```

## Scanner
In order to read data from Fluss tables, first you need to create a Scanner instance. Then users can subscribe to the table buckets and 
start polling for records.
```java
LogScanner logScanner = table.newScan()
        .createLogScanner();

int numBuckets = table.getTableInfo().getNumBuckets();
System.out.println("Number of buckets: " + numBuckets);
for (int i = 0; i < numBuckets; i++) {     
    System.out.println("Subscribing to bucket " + i);
    logScanner.subscribeFromBeginning(i);
}

long scanned = 0;
Map<Integer, List<String>> rowsMap = new HashMap<>();

while (true) {     
    System.out.println("Polling for records...");
    ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
    for (TableBucket bucket : scanRecords.buckets()) {
        for (ScanRecord record : scanRecords.records(bucket)) {
            InternalRow row = record.getRow();
            System.out.println(row.toString());
        }
    }
    scanned += scanRecords.count();
}
```

## Lookup
You can also use the Fluss API to perform lookups on a table. This is useful for querying specific records based on their primary key.
```java
LookupResult lookup = table.newLookup().createLookuper().lookup(rowKey).get();
```