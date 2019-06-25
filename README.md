ACID Datasource a datasource built on top of Spark Datasource V1 APIs, and provides Spark support for 
[Hive ACID transactions](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions).

This datasource provides the capability to work with Hive ACID V2 tables, both Full ACID tables as well as Insert-Only 
tables. Currently, it supports reading from these ACID tables only, and ability to write (insert, insert overwrite) will soon be added in the near future. 

Please refer to [Quick Start](#Quick Start) for details on getting started with using this Datasource.

# Latest Binaries

ACID datasource is published to Maven Central Repository and can be used by adding a dependency in your POM file.

     <depenencies>
       <dependency>
         <groupId>spark-acid-ds</groupId>
         <artifactId>acid-ds-core_2.11</artifactId>
         <version>0.1.0</version>
       </dependency>
       <dependency>
         <groupId>shaded-os-dependencies</groupId>
         <artifactId>shaded-acid-ds-dependency_2.11</artifactId>
         <version>0.1.0</version>
       </dependency>
     </dependencies>

## Version Compatibility

### Compatibility with Apache Spark Versions

ACID datasource has been tested to work with Apache Spark 2.4.3, but it should work with older versions as well. 
However, because of a Hive dependency, this datasource needs Hadoop version 2.8.2 or higher due to [HADOOP-14683](https://jira.apache.org/jira/browse/HADOOP-14683)

### Data Storage Compatibility

1. ACID datasource does not control data storage format and layout, which is managed by Hive. It should be able to work 
with data written by Hive version 3.0.0 and above. Please see [Hive ACID storage layout](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions#HiveTransactions-BasicDesign).
2. ACID datasource works with data stored on local files, HDFS as well as cloud blobstores (AWS S3, Azure Blob Storage 
etc).

# Building

This project has the following sbt projects:

1. **acid-datasource**: The main project for the datasource. This has the actual code for the datasource, which 
implements the interaction with Hive ACID transaction and HMS subsystem.
2. **shaded-acid-ds-dependencies**: This is an sbt project to create the shaded hive metastore and hive exec jars 
combined into a fat jar(shaded-oss-dependencies-assembly-0.1.jar referred below). This jar has been created already and packaged into the lib folder of acid-datasource as an umanaged dependency. This is required due to our dependency on Hive 3 for Hive ACID, and Spark currently only supports Hive 1.2


To compile:

```bash
cd acid-datasource
sbt package
cd hive-shaded-exec-metastore
sbt assembly
```

# Testing

Tests run against a standalone docker setup. Please refer to [Docker setup] (docker/README.md) to build and start a container. 

_NB: Container run HMS server and HDFS and listens on port 10000 and 9000. So stop if you are running HMS or HDFS on same port on host machine._

To run unit test,

```bash
sbt test
```

Refer to [SBT docs](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html) for more commands.

# Quick Start

These are pre-requisites to using this library

1. That you already have Hive table (ACID V2) data somewhere and need to read it from Spark (Currently write is 
not _NOT_ supported).
2. You have Hive Metastore DB with version 3.0.0 or higher. Please refer to 
[Hive Metastore](https://cwiki.apache.org/confluence/display/Hive/Design#Design-MetastoreArchitecture) for details.
3. You have a Hive Metastore Server running with version 3.0.0 or higher, as Hive ACID needs a standalone Hive 
Metastore Server to operate. Please refer to 
[Hive Configuration](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions#HiveTransactions-Configuration) 
for configuration options.
4. You are using the above Hive Metastore Server with your Spark for its metastore communications.

## Config

Change configuration in `$SPARK_HOME/conf/hive-site.xml` to point to already configured HMS server endpoint. 
If you meet the above pre-requisites, this is probably already configured.

```xml
<configuration>
  <property>
  <name>hive.metastore.uris</name>
    <!-- hostname must point to the Hive metastore URI in your cluster -->
    <value>thrift://hostname:9083</value>
    <description>URI for client to contact metastore server</description>
  </property>
</configuration>
```

## Run

There are couple of ways to use the library while running spark-shell

1. Pass it as part of command line

`spark-shell --jars ${ACID_DS_HOME}/acid-datasource/target/scala-2.11/acid-datasource_2.11-0.1.jar,
${ACID_DS_HOME}/acid-datasource/lib/shaded-oss-dependencies-assembly-0.1.jar`

2. Copy the acid-datasource and shaded-oss-dependencies jar to `$SPARK_HOME/assembly/target/scala.2_11/jars` and run

`spark-shell`

### SQL
To read an existing Hive ACID table through SparkSQL, you need to create a symlink table against it. This symlink is 
required to instruct Spark to use this datasource against an existing table.

To create the symlink table

`scala> spark.sql("create table symlinkacidtable using com.qubole.spark.datasources.hiveacid.HiveAcidDataSource options 
('table' 'default.acidtbl')")`

To read

`scala> var df = spark.sql("select * from symlinkacidtable")`

`scala> df.collect()`

### scala

To read table from scala / pySpark table can be directly accessed.

`scala> val df = spark.read.format("com.qubole.spark.datasources.hiveacid.HiveAcidDataSource").options(Map("table" -> 
"default.acidtbl")).load()`

`scala> df.collect()`

## Design <TBD>

## Transactional Guarantees

Spark ACID datasource uses [Hive Transaction] (https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions) to 
provide transactional guarantees over it.

###1. Snapshot Isolation
When you start reading data from an acid table using this datasource, the snapshot of data to be read is acquired lazily
 just before query execution. In RDD terms, the snapshot to be read is acquired just when computing the partitions for 
 the RDD lazily, and is maintained (or cached)
in the RDD throughout its lifetime. This means that if you end up reusing the RDD or recomputing some partitions of the 
RDD, you will see consistent data for the lifetime of that RDD.

###2. Locks
Hive ACID works with locks, where every client that is operating on ACID tables is expected to acquire locks for the 
duration of reads and writes.
This datasource however does not acquire read locks. When it needs to read data, it talks to the HiveMetaStore Server to get
the list of transactions that have been committed, and using that, the list of files it should read from the filesystem. 
But it does not lock the table or partition for the duration of the read.

Because it does not acquire read locks, there is a chance that the data being read could get deleted by Hive's ACID 
management(perhaps because the data was ready to be cleaned up due to compaction).
To avoid this scenario which can read to query failures, we recommend that you disable automatic compaction and cleanup 
in Hive on the tables that you are going to be reading using this datsource, and recommend that the compaction and 
cleanup be done when you know that no users are reading those tables. 

We are looking into taking read locks to remove this restriction, and hope to be able to fix this in the near future.

## Contributing

We use [BitBucket Issues](https://bitbucket.com/qubole/acid-ds) to track issues.

## Reporting bugs or feature requests

Please use the github issues for the spark-acid-ds project to report issues or raise feature requests.
