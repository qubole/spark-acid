# Hive ACID Data Source for Apache Spark

A Datasource on top of Spark Datasource V1 APIs, that provides Spark support for [Hive ACID transactions](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions).

This datasource provides the capability to work with Hive ACID V2 tables, both Full ACID tables as well as Insert-Only tables. Currently, it supports reading from these ACID tables only, and ability to write will be added in the near future.

## Quick Start

These are the pre-requisites to using this library:

1. You already have Hive ACID tables (ACID V2) and need to read it from Spark (as currently write is not _NOT_ supported).
2. You have Hive Metastore DB with version 3.0.0 or higher. Please refer to [Hive Metastore](https://cwiki.apache.org/confluence/display/Hive/Design#Design-MetastoreArchitecture) for details.
3. You have a Hive Metastore Server running with version 3.0.0 or higher, as Hive ACID needs a standalone Hive Metastore Server to operate. Please refer to [Hive Configuration](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions#HiveTransactions-Configuration) for configuration options.
4. You are using the above Hive Metastore Server with your Spark for its metastore communications.

### Config

Change configuration in `$SPARK_HOME/conf/hive-site.xml` to point to already configured HMS server endpoint. If you meet the above pre-requisites, this is probably already configured.

```xml
<configuration>
  <property>
  <name>hive.metastore.uris</name>
    <!-- hostname must point to the Hive metastore URI in your cluster -->
    <value>thrift://hostname:10000</value>
    <description>URI for spark to contact the hive metastore server</description>
  </property>
</configuration>
```

### Run

There are a few ways to use the library while running spark-shell

1. Use the published package 

       spark-shell --package qubole:spark-acid_2.11:0.1.0

2. If you built the jar yourself, copy the `spark-acid-0.1.0.jar` jar into `$SPARK_HOME/assembly/target/scala.2_11/jars` and run

       spark-shell
        
#### Scala/Python

To read the acid table from Scala / pySpark, the table can be directly accessed using this datasource. 
Note the short name of this datasource is `HiveAcid`

    scala> val df = spark.read.format("HiveAcid").options(Map("table" -> "default.acidtbl")).load()
    scala> df.collect()

#### SQL
To read an existing Hive acid table through pure SQL, you need to create a dummy table that acts as a symlink to the
original acid table. This symlink is required to instruct Spark to use this datasource against an existing table.

To create the symlink table

    scala> spark.sql("create table symlinkacidtable using HiveAcid options ('table' 'default.acidtbl')")

_NB: This will produce a warning indicating that Hive does not understand this format_

    WARN hive.HiveExternalCatalog: Couldn’t find corresponding Hive SerDe for data source provider com.qubole.spark.datasources.hiveacid.HiveAcidDataSource. Persisting data source table `default`.`sparkacidtbl` into Hive metastore in Spark SQL specific format, which is NOT compatible with Hive.

_Please ignore it, as this is a sym table for Spark to operate with and no underlying storage._

To read the table data:

    scala> var df = spark.sql("select * from symlinkacidtable")
    scala> df.collect()


## Latest Binaries

ACID datasource is published spark-packages.org. The latest version of the binary is `0.1.0`


## Version Compatibility

### Compatibility with Apache Spark Versions

ACID datasource has been tested to work with Apache Spark 2.4.3, but it should work with older versions as well. However, because of a Hive dependency, this datasource needs Hadoop version 2.8.2 or higher due to [HADOOP-14683](https://jira.apache.org/jira/browse/HADOOP-14683)

_NB: Hive ACID is supported in Hive 3.1.1 onwards and for that hive Metastore db needs to be [upgraded](https://cwiki.apache.org/confluence/display/Hive/Hive+Schema+Tool) to 3.1.1._

### Data Storage Compatibility

1. ACID datasource does not control data storage format and layout, which is managed by Hive. It works with data written by Hive version 3.0.0 and above. Please see [Hive ACID storage layout](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions#HiveTransactions-BasicDesign).

2. ACID datasource works with data stored on local files, HDFS as well as cloud blobstores (AWS S3, Azure Blob Storage etc).

## Developer resources
### Build

This project has the following sbt projects:

* **shaded-dependencies**: This is an sbt project to create the shaded hive metastore and hive exec jars combined into a fat jar `spark-acid-shaded-dependencies`. This is required due to our dependency on Hive 3 for Hive ACID, and Spark currently only supports Hive 1.2

To compile and publish shaded dependencies jar:

    sbt clean publishLocal

* **acid-datasource**: The main project for the datasource. This has the actual code for the datasource, which implements the interaction with Hive ACID transaction and HMS subsystem.

To compile, first publish the dependencies jar locally as mentioned above. After that:

    sbt package


Tests are run against a standalone docker setup. Please refer to [Docker setup] (docker/README.md) to build and start a container.

_NB: Container run HMS server, HS2 Server and HDFS and listens on port 10000,10001 and 9000 respectively. So stop if you are running HMS or HDFS on same port on host machine._

To run the full integration test:

    sbt test


### Release

To release a new version use

    sbt release

To publish a new version use

    sbt spPublish

Read more about [sbt release](https://github.com/sbt/sbt-release)


### Design Constraints

Hive ACID works with locks, where every client that is operating on ACID tables is expected to acquire locks for the duration of reads and writes. This datasource however does not acquire read locks. When it needs to read data, it talks to the HiveMetaStore Server to get the list of transactions that have been committed, and using that, the list of files it should read from the filesystem. But it does not lock the table or partition for the duration of the read.

Because it does not acquire read locks, there is a chance that the data being read could get deleted by Hive's ACID management(perhaps because the data was ready to be cleaned up due to compaction). To avoid this scenario which can read to query failures, we recommend that you disable automatic compaction and cleanup in Hive on the tables that you are going to be reading using this datasource, and recommend that the compaction and cleanup be done when you know that no users are reading those tables. Ideally, we would have wanted to just disable automatic cleanup and let the compaction happen, but there is no way in Hive today to just disable cleanup and it is tied to compaction, so we recommend to disable compaction.

You have a few options available to you to disable automatic compaction:

1. Disable automatic compaction globally, i.e. for all ACID tables: To do this, we recommend you set the following compaction thresholds on the Hive Metastore Server to a very high number(like 1000000 below) so that compaction never gets initiated automatically and can only be initiated manually.

        hive.compactor.delta.pct.threshold=1000000
        hive.compactor.delta.num.threshold=1000000

2. Disable automatic compaction for selected ACID tables: To do this, you can set a table property using the ALTER TABLE command:

        ALTER TABLE <> SET TBLPROPERTIES ("NO_AUTO_COMPACTION"="true")  

This will disable automatic compaction on a particular table, and you can use this approach if you have a limited set of ACID tables that you intend to access using this datasource.

Once you have disabled automatic compaction either globally or on a particular set of tables, you can chose to run compaction manually at a desired time when you know there are no readers reading these acid tables, using an ALTER TABLE command:

        ALTER TABLE table_name [PARTITION (partition_key = 'partition_value' [, ...])] COMPACT 'compaction_type'[AND WAIT] [WITH OVERWRITE TBLPROPERTIES ("property"="value" [, ...])];

compaction_type are either `MAJOR` or `MINOR`

More details on the above commands and their variations available [here](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL).

We are looking into removing this restriction, and hope to be able to fix this in the near future.

## Contributing

We use [Github Issues](https://github.com/qubole/spark-acid/issues) to track issues.

## Reporting bugs or feature requests

Please use the github issues for the spark-acid project to report issues or raise feature requests.
