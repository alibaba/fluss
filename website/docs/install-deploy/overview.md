---
sidebar_position: 1
---

# Overview

Below, we provide an overview of the key components of a Fluss cluster, detailing their functionalities and implementations. Additionally, we will introduce the various deployment methods available for Fluss.

## Overview and Reference Architecture

The figure below shows the building blocks of Fluss clusters:

<img width="1200px" src={require('./deployment_overview.png').default} />



When deploying Fluss, there are often multiple options available for each building block.
We have listed them in the table below the figure.


<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" width="250">Component</th>
      <th class="text-left" width="600">Purpose</th>
      <th class="text-left" width="300">Implementations</th>
    </tr>
   </thead>
   <tbody>
        <tr>
            <td>Fluss Client</td>
            <td>
                <p>
                    The Fluss Client is the entry point for users to interact with Fluss Cluster. It is responsible for 
                    managing Fluss Cluster like:
                </p>
                <ul>
                    <li> Admin operation: like create or delete database/table etc</li>
                    <li>Table operation: like write, read, delete data</li>
                </ul>
            </td>
            <td>
                <ul>
                    <li>[Flink Connector](/docs/engine-flink/getting-started/)</li>
                </ul>
            </td>
        </tr>
        <tr>
            <td>CoordinatorServer</td>
            <td>
                <p>
                CoordinatorServer is the name of the central work coordination component of Fluss. 
                The coordinator server is responsible to:
                </p>
                <ul>
                    <li>Manage the TabletServer</li>
                    <li>Manage the metadata</li>
                    <li>Coordinate the whole cluster, e.g. data re-balance, recover data when tablet servers down</li>
                </ul>
            </td>
            <td rowspan="2">
                <ul>
                    <li>[Local Cluster](/docs/install-deploy/deploying-local-cluster/)</li>
                    <li>[Distributed Cluster](/docs/install-deploy/deploying-distributed-cluster/)</li>
                    <li>[Docker run / Docker compose](/docs/install-deploy/deploying-with-docker/)</li>
                </ul>
            </td>
        </tr>
        <tr>
            <td>TabletServer</td>
            <td>
                <p>
                TabletServers are the actual node to manage and store data.
                </p>
            </td>
        </tr>
        <tr>
            <td colspan="3" style={{ textAlign: "center" }}>
                <b>External Components</b>
            </td>
        </tr>
            <tr>
                <td>ZooKeeper</td>
                    <td>
                        :::warning
                        Zookeeper will be removed to simplify deployment in the near future. For more details, please checkout [Roadmap](/roadmap/).
                        :::
                        <p>
                        Fluss leverages ZooKeeper for distributed coordination between all running CoordinatorServer instances and for metadata management.
                        </p>
                    </td>
                    <td>
                        <ul>
                            <li><a href="https://zookeeper.apache.org/">Zookeeper</a></li>
                        </ul>
                    </td>
                </tr>
            <tr>
            <td>Remote Storage (optional)</td>
            <td>
                Fluss uses file systems as remote storage to store snapshots for Primary-Key Table and store tiered log segments for Log Table.
            </td>
            <td>
            <li>[HDFS](/docs/maintenance/filesystems/hdfs/)</li>
            <li>[Aliyun OSS](/docs/maintenance/filesystems/oss/)</li>
            <li>[Amazon S3](/docs/maintenance/filesystems/s3/)</li>
            </td>
        </tr>
        <tr>
            <td>Lakehouse Storage (optional)</td>
            <td>
               Fluss's DataLake Tiering Service will continuously compact Fluss's Arrow files into Parquet/ORC files in open lake format.
               The data in Lakehouse storage can be read both by Fluss's client in a Union Read manner and accessed directly
               by query engines such as Flink, Spark, StarRocks, Trino.
            </td>
            <td>
            <li>[Paimon](/docs/maintenance/tiered-storage/lakehouse-storage/)</li>
            <li>[Iceberg (Roadmap)](/roadmap/)</li>
            </td>
        </tr>
        <tr>
            <td>Metrics Storage (optional)</td>
            <td>
                CoordinatorServer/TabletServer report internal metrics and Fluss client (e.g., connector in Flink jobs) can report additional, client specific metrics as well.
            </td>
            <td>
               <li>[JMX](/docs/maintenance/metric-reporters#jmx)</li>
               <li>[Prometheus](/docs/maintenance/metric-reporters#prometheus)</li>
            </td>
        </tr>
    </tbody>
</table>

## How to deploy Fluss

Fluss can be deployed in three different ways:
- [Local Cluster](/docs/install-deploy/deploying-local-cluster/)
- [Distributed Cluster](/docs/install-deploy/deploying-distributed-cluster/)
- [Docker run/ Docker compose](/docs/install-deploy/deploying-with-docker/)

**NOTE**:
- Local Cluster is for testing purpose only.