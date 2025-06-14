---
title: Security QuickStart
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

#  Security QuickStart
This document describes how to quickly deploy a security cluster and use it in Flink.

## Deploy with Docker Compose
### Create docker-compose.yml file
You can use the following docker-compose.yml file to start a Fluss cluster with one CoordinatorServer and one TabletServer.

In security properties, it shows that this cluster uses SASL/PLAIN to authenticate users.And there are two users: admin and guest.The admin user is the super user which have all the permissions.

```yaml
services:
  coordinator-server:
    image: fluss/fluss:$FLUSS_VERSION$
    command: coordinatorServer
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: INTERNAL://coordinator-server:0, CLIENT://coordinator-server:9123
        advertised.listeners: CLIENT://localhost:9123
        internal.listener.name: INTERNAL
        remote.data.dir: /tmp/fluss/remote-data
        # security properties
        security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
        security.sasl.enabled.mechanisms: PLAIN
        security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_guest="guest-pass";
        authorizer.enabled: true
        super.users: User:admin
    ports:
      - "9123:9123"
  tablet-server:
    image: fluss/fluss:$FLUSS_VERSION$
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: INTERNAL://tablet-server:0, CLIENT://tablet-server:9123
        advertised.listeners: CLIENT://localhost:9124
        internal.listener.name: INTERNAL
        tablet-server.id: 0
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data
        remote.data.dir: /tmp/fluss/remote-data
        # security properties
        security.protocol.map: CLIENT:SASL, INTERNAL:PLAINTEXT
        security.sasl.enabled.mechanisms: PLAIN
        security.sasl.plain.jaas.config: com.alibaba.fluss.security.auth.sasl.plain.PlainLoginModule required user_admin="admin-pass" user_fluss="fluss-pass";
        authorizer.enabled: true
        super.users: User:admin
    ports:
        - "9124:9123"
    volumes:
      - shared-tmpfs:/tmp/fluss
  zookeeper:
    restart: always
    image: zookeeper:3.9.2

volumes:
  shared-tmpfs:
    driver: local
    driver_opts:
      type: "tmpfs"
      device: "tmpfs"
```

### Launch the components

Save the `docker-compose.yaml` script and execute the `docker compose up -d` command in the same directory
to create the cluster.

Run the below command to check the container status:

```bash
docker container ls -a
```

## Prepare Flink Environment
### Start Flink Cluster
You can start a Flink standalone cluster refer to [Flink Environment Preparation](engine-flink/getting-started.md#preparation-when-using-flink-sql-client)

**Note**: Make sure the [Fluss connector jar](/downloads/) already has copied to the `lib` directory of your Flink home.
```shell
bin/start-cluster.sh 
```


### Enter into SQL-Client
Use the following command to enter the Flink SQL CLI Container:
```shell
bin/sql-client.sh
```

## Create Catalog with Authentication
At first, we create a catalog without authentication setting:
```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);
```
It will show that the client.security.protocol' is not right.
```sql
[ERROR] Could not execute SQL statement. Reason:
com.alibaba.fluss.exception.AuthenticationException: The connection has not completed authentication yet. This may be caused by a missing or incorrect configuration of 'client.security.protocol' on the client side.
```

Then, we set the authentication properties as user `guest` in the catalog, then it will work:
```sql title="Flink SQL"
CREATE CATALOG guest_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'localhost:9123',
  'client.security.protocol' = 'SASL',
  'client.security.sasl.mechanism' = 'PLAIN',
  'client.security.sasl.username' = 'guest',
  'client.security.sasl.password' = 'guest-pass'
);
```

Aslo, we set the authentication properties as user `admin` in the catalog, then it will work:
```sql title="Flink SQL"
CREATE CATALOG admin_catalog WITH (
'type' = 'fluss',
'bootstrap.servers' = 'localhost:9123',
'client.security.protocol' = 'SASL',
'client.security.sasl.mechanism' = 'PLAIN',
'client.security.sasl.username' = 'admin',
'client.security.sasl.password' = 'admin-pass'
);
```

## Create Table with user `guest`
Then, we can use the `guest_catalog` to show databases:
```sql title="Flink SQL"
USE CATALOG guest_catalog;
SHOW DATABASES;
```
The result is empty set. It seems unnormal bacause the fluss cluster has a default database named `fluss`.

Then we try to create a table:
```sql title="Flink SQL"
CREATE TABLE `guest_catalog`.`fluss`.`fluss_order` (
    `order_key` BIGINT,
    `cust_key` INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    `order_date` DATE,
    `order_priority` STRING,
    `clerk` STRING,
    `ptime` AS PROCTIME(),
    PRIMARY KEY (`order_key`) NOT ENFORCED
);
```
The result is:
```sql
[ERROR] Could not execute SQL statement. Reason:
com.alibaba.fluss.exception.AuthorizationException: Principal FlussPrincipal{name='guest', type='User'} have no authorization to operate CREATE on resource Resource{type=DATABASE, name='fluss'}
```

it turned out that the user `guest` have no authorization for `CREATE` operation  on database `fluss`.

## Create Table with user `admin`
Then, we can use the `guest_catalog` to show databases:
```sql title="Flink SQL"
USE CATALOG admin_catalog;
SHOW DATABASES;
```
The result show that the database `fluss` exists.
```sql
+---------------+
| database name |
+---------------+
|         fluss |
+---------------+
```
Then we try to create a table:
```sql title="Flink SQL"
CREATE TABLE `admin_catalog`.`fluss`.`fluss_order` (
    `order_key` BIGINT,
    `cust_key` INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    `order_date` DATE,
    `order_priority` STRING,
    `clerk` STRING,
    `ptime` AS PROCTIME(),
    PRIMARY KEY (`order_key`) NOT ENFORCED
);
```
Then this statement will execute successfully. It means the user `admin` have authorization for `CREATE` operation  on database `fluss`.

## Add Acl for user `guest`
We can add acl for user `guest` in two ways:
```sql title="Flink SQL"
CALL admin_catalog.sys.add_acl(
    'cluster.fluss', 
    'ALLOW',
    'User:fluss', 
    'CREATE',
    '*'
);

-- This can only used for flink 1.19 and above.
CALL admin_catalog.sys.add_acl(
    resource => 'cluster.fluss', 
    permission => 'ALLOW',
    principal => 'User:guest', 
    operation => 'CREATE',
    host => '*'
);
```

Then we list the acls:
```sql title="Flink SQL"
CALL admin_catalog.sys.list_acl(
    'cluster.fluss', 
    'ANY',
    'ANY', 
    'ANY',
    'ANY'
);

CALL admin_catalog.sys.list_acl(
    resource => 'cluster.fluss'
);
```

The result show that user `guest` hava authorization for `CREATE` operation  on database `fluss` now.
```sql
+--------------------------------------------------------------------------------------------+
|                                                                                     result |
+--------------------------------------------------------------------------------------------+
| resourceType="fluss";permission="ALLOW";principal="User:guest";operation="CREATE";host="*" |
+--------------------------------------------------------------------------------------------+
1 row in set
```

Then we use the `guest_catalog` to create a table again:
```sql title="Flink SQL"
CREATE TABLE `guest_catalog`.`fluss`.`fluss_order2` (
    `order_key` BIGINT,
    `cust_key` INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    `order_date` DATE,
    `order_priority` STRING,
    `clerk` STRING,
    `ptime` AS PROCTIME(),
    PRIMARY KEY (`order_key`) NOT ENFORCED
);
```
Then this statement will execute successfully. 

## Drop Acl for user `guest`
We can remove acl for user `guest` in two ways:
```sql title="Flink SQL"
CALL admin_catalog.sys.drop_acl(
    'cluster.fluss', 
    'ALLOW',
    'User:guest', 
    'CREATE',
    'ANY'
);

CALL admin_catalog.sys.drop_acl(
    resource => 'cluster.fluss', 
    permission => 'ALLOW',
    principal => 'User:guest', 
    operation => 'CREATE'
);
```

Then we list the acls:
```sql title="Flink SQL"
CALL admin_catalog.sys.list_acl(
    'cluster.fluss', 
    'ANY',
    'ANY', 
    'ANY',
    'ANY'
);

CALL admin_catalog.sys.list_acl(
    resource => 'cluster.fluss'
);
```

The result is empty set which means user `guest` have no authorization for `CREATE` operation  on database `fluss` now.

Then we use the `guest_catalog` to create a table again:
```sql title="Flink SQL"
CREATE TABLE `guest_catalog`.`fluss`.`fluss_order3` (
    `order_key` BIGINT,
    `cust_key` INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    `order_date` DATE,
    `order_priority` STRING,
    `clerk` STRING,
    `ptime` AS PROCTIME(),
    PRIMARY KEY (`order_key`) NOT ENFORCED
);
```

The result is:
```sql
[ERROR] Could not execute SQL statement. Reason:
com.alibaba.fluss.exception.AuthorizationException: Principal FlussPrincipal{name='guest', type='User'} have no authorization to operate CREATE on resource Resource{type=DATABASE, name='fluss'}
```