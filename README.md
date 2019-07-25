# Hive ACID Data Source for Apache Spark

Datasource for Spark on the top of Spark Datasource V1 APIs, and provides Spark support for [Hive ACID transactions](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions).

This datasource provides the capability to work with Hive ACID V2 tables, both Full ACID tables as well as Insert-Only tables. Currently, it supports reading from these ACID tables only, and ability to write (insert, insert overwrite) will soon be added in the near future.

Please refer to [Quick Start](#Quick Start) for details on getting started with using this Datasource.


# Latest Binaries

ACID datasource is published spark-packages.org. It can be used as spark package

`spark-shell --package com.qubole:spark-acid_2.11:0.1`

## Version Compatibility

### Compatibility with Apache Spark Versions

ACID datasource has been tested to work with Apache Spark 2.4.3, but it should work with older versions as well. However, because of a Hive dependency, this datasource needs Hadoop version 2.8.2 or higher due to [HADOOP-14683](https://jira.apache.org/jira/browse/HADOOP-14683)

_NB: Hive ACID is supported in Hive 3.1.1 onwards and for that hive Metastore db needs to be [upgraded](https://cwiki.apache.org/confluence/display/Hive/Hive+Schema+Tool) to 3.1.1._

### Data Storage Compatibility

1. ACID datasource does not control data storage format and layout, which is managed by Hive. It works with data written by Hive version 3.0.0 and above. Please see [Hive ACID storage layout](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions#HiveTransactions-BasicDesign).

2. ACID datasource works with data stored on local files, HDFS as well as cloud blobstores (AWS S3, Azure Blob Storage etc).

# Building

This project has the following sbt projects:

1. **shaded-dependencies**: This is an sbt project to create the shaded hive metastore and hive exec jars combined into a fat jar(spark-acid-shaded-dependencies-assembly-0.1.jar referred below). This jar has been created already and packaged into the lib folder of acid-datasource as an umanaged dependency. This is required due to our dependency on Hive 3 for Hive ACID, and Spark currently only supports Hive 1.2

To compile and publish shaded dependencies jar:

```bash
cd shaded-dependencies
sbt clean publishLocal
```
This would create and publish `spark-acid-shaded-dependencies_2.11-assembly.jar` which has all the runtime and test dependencies.


2. **acid-datasource**: The main project for the datasource. This has the actual code for the datasource, which implements the interaction with Hive ACID transaction and HMS subsystem.

To compile acid-datasource

```bash
sbt acid_datasource/package
```

Tests run against a standalone docker setup. Please refer to [Docker setup] (docker/README.md) to build and start a container.

_NB: Container run HMS server, HS2 Server and HDFS and listens on port 10000,10001 and 9000 respectively. So stop if you are running HMS or HDFS on same port on host machine._

To run full integration test,

```bash
sbt acid_datasource/test
```

# Release

To release a new version use

```bash
sbt release
```

Read more about [sbt release](https://github.com/sbt/sbt-release)

# Publishing

To publish fully assembled jar to spark package

```bash
sbt spPublish
```

Refer to [SBT docs](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html) for more commands.

# Quick Start

These are pre-requisites to using this library

1. That you already have Hive table (ACID V2) data somewhere and need to read it from Spark (Currently write is not _NOT_ supported).
2. You have Hive Metastore DB with version 3.0.0 or higher. Please refer to [Hive Metastore](https://cwiki.apache.org/confluence/display/Hive/Design#Design-MetastoreArchitecture) for details.
3. You have a Hive Metastore Server running with version 3.0.0 or higher, as Hive ACID needs a standalone Hive Metastore Server to operate. Please refer to [Hive Configuration](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions#HiveTransactions-Configuration) for configuration options.
4. You are using the above Hive Metastore Server with your Spark for its metastore communications.

## Config

Change configuration in `$SPARK_HOME/conf/hive-site.xml` to point to already configured HMS server endpoint. If you meet the above pre-requisites, this is probably already configured.

```xml
<configuration>
  <property>
  <name>hive.metastore.uris</name>
    <!-- hostname must point to the Hive metastore URI in your cluster -->
    <value>thrift://hostname:10001</value>
    <description>URI for client to contact metastore server</description>
  </property>
</configuration>
```

## Run

There are couple of ways to use the library while running spark-shell

1. Pass it as part of command line

`spark-shell --jars ${ACID_DS_HOME}/acid-datasource/target/scala-2.11/spark-acid-assembly-0.1-SNAPSHOT.jar`

2. Copy the acid-datasource assembly jar intto `$SPARK_HOME/assembly/target/scala.2_11/jars` and run

`spark-shell`

### SQL
To read an existing Hive ACID table through SparkSQL, you need to create a symlink table against it. This symlink is required to instruct Spark to use this datasource against an existing table.

To create the symlink table

`scala> spark.sql("create table symlinkacidtable using HiveAcid options ('table' 'default.acidtbl')")`

_NB: This will produce a warning indicating that Hive does not understand this format

`WARN hive.HiveExternalCatalog: Couldn’t find corresponding Hive SerDe for data source provider com.qubole.spark.datasources.hiveacid.HiveAcidDataSource. Persisting data source table `default`.`sparkacidtbl` into Hive metastore in Spark SQL specific format, which is NOT compatible with Hive.`

Please ignore it, as this is a sym table for Spark to operate with and no underlying storage._

To read

`scala> var df = spark.sql("select * from symlinkacidtable")`

`scala> df.collect()`

### scala

To read table from scala / pySpark table can be directly accessed.

`scala> val df = spark.read.format("HiveAcid").options(Map("table" -> "default.acidtbl")).load()`

`scala> df.collect()`

## Design <TBD>

## Transactional Guarantees

Spark ACID datasource uses [Hive Transaction](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions) to provide transactional guarantees over it.

###1. Snapshot Isolation
When you start reading data from an acid table using this datasource, the snapshot of data to be read is acquired lazily just before query execution. In RDD terms, the snapshot to be read is acquired just when computing the partitions for the RDD lazily, and is maintained (or cached) in the RDD throughout its lifetime. This means that if you end up reusing the RDD or recomputing some partitions of the RDD, you will see consistent data for the lifetime of that RDD.

###2. Locks
Hive ACID works with locks, where every client that is operating on ACID tables is expected to acquire locks for the duration of reads and writes. This datasource however does not acquire read locks. When it needs to read data, it talks to the HiveMetaStore Server to get the list of transactions that have been committed, and using that, the list of files it should read from the filesystem. But it does not lock the table or partition for the duration of the read.

Because it does not acquire read locks, there is a chance that the data being read could get deleted by Hive's ACID management(perhaps because the data was ready to be cleaned up due to compaction). To avoid this scenario which can read to query failures, we recommend that you disable automatic compaction and cleanup in Hive on the tables that you are going to be reading using this datasource, and recommend that the compaction and cleanup be done when you know that no users are reading those tables. Ideally, we would have wanted to just disable automatic cleanup and let the compaction happen, but there is no way in Hive today to just disable cleanup and it is tied to compaction, so we recommend to disable compaction.

You have a few options available to you to disable automatic compaction:

1. Disable automatic compaction globally, i.e. for all ACID tables: To do this, we recommend you set the following compaction thresholds on the Hive Metastore Server to a very high number(like 1000000 below) so that compaction never gets initiated automatically and can only be initiated manually.

`hive.compactor.delta.pct.threshold=1000000`
`hive.compactor.delta.num.threshold=1000000`

2. Disable automatic compaction for selected ACID tables: To do this, you can set a table property using the ALTER TABLE command:

`ALTER TABLE <> SET TBLPROPERTIES ("NO_AUTO_COMPACTION"="true")`

This will disable automatic compaction on a particular table, and you can use this approach if you have a limited set of ACID tables that you intend to access using this datasource.

Once you have disabled automatic compaction either globally or on a particular set of tables, you can chose to run compaction manually at a desired time when you know there are no readers reading these acid tables, using an ALTER TABLE command:

`ALTER TABLE table_name [PARTITION (partition_key = 'partition_value' [, ...])] COMPACT 'compaction_type'[AND WAIT] [WITH OVERWRITE TBLPROPERTIES ("property"="value" [, ...])];`

compaction_type are either `MAJOR` or `MINOR`

More details on the above commands and their variations available [here](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL).

We are looking into removing this restriction, and hope to be able to fix this in the near future.

## Contributing

We use [Github Issues](https://github.com/qubole/spark-acid) to track issues.

## Reporting bugs or feature requests

Please use the github issues for the spark-acid-ds project to report issues or raise feature requests.
