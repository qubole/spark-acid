# Hive ACID Data Source for Apache Spark

A Datasource on top of Spark Datasource V1 APIs, that provides Spark support for [Hive ACID transactions](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions).

This datasource provides the capability to work with Hive ACID V2 tables, both Full ACID tables as well as Insert-Only tables.

functionality availability matrix

Functionality | Full ACID table | Insert Only Table |
------------- | --------------- | ----------------- |
READ | >= v0.4.0 | >= v0.4.0 |
INSERT INTO / OVERWRITE | >= v0.4.3 | >=0.4.5 |
CTAS  | >= v0.4.3 | >=0.4.5 |
UPDATE  | >=0.4.5 | Not Supported |
DELETE | >=0.4.5 | Not Supported |
MERGE | Not Supported | Not Supported |

*Note: In case of insert only table for support of write operation compatibility check needs to be disabled*

## Quick Start

These are the pre-requisites to using this library:

1. You have Hive Metastore DB with version 3.1.2 or higher. Please refer to [Hive Metastore](https://cwiki.apache.org/confluence/display/Hive/Design#Design-MetastoreArchitecture) for details.
2. You have a Hive Metastore Server running with version 3.1.1 or higher, as Hive ACID needs a standalone Hive Metastore Server to operate. Please refer to [Hive Configuration](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions#HiveTransactions-Configuration) for configuration options.

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

       `spark-shell --packages qubole:spark-acid:0.4.2-s_2.11

2. If you built the jar yourself, copy the `spark-acid-assembly-0.4.2.jar` jar into `$SPARK_HOME/assembly/target/scala.2_11/jars` and run

       spark-shell

#### Scala/Python

To operate on Hive ACID table from Scala / pySpark, the table can be directly accessed using this datasource. Note the short name of this datasource is `HiveAcid`. Hive ACID table are tables in HiveMetastore so any operation of read and/or write needs `format("HiveAcid").option("table", "<table name>"")`. _Direct read and write from the file is not supported_

    scala> val df = spark.read.format("HiveAcid").options(Map("table" -> "default.acidtbl")).load()
    scala> df.collect()

#### SQL

To read an existing Hive acid table through pure SQL, there are two ways:

1. Create a dummy table that acts as a symlink to the original acid table. This symlink is required to instruct Spark to use this datasource against an existing table.

	To create the symlink table:

		spark.sql("create table symlinkacidtable using HiveAcid options ('table' 'default.acidtbl')")

        spark.sql("select * from symlinkacidtable")


	_NB: This will produce a warning indicating that Hive does not understand this format_

     WARN hive.HiveExternalCatalog: Couldn’t find corresponding Hive SerDe for data source provider com.qubole.spark.hiveacid.datasource.HiveAcidDataSource. Persisting data source table `default`.`sparkacidtbl` into Hive metastore in Spark SQL specific format, which is NOT compatible with Hive.

_Please ignore it, as this is a sym table for Spark to operate with and no underlying storage._

2. Use SparkSession extensions framework to add a new Analyzer rule (HiveAcidAutoConvert) to Spark Analyser. This analyzer rule automatically converts an _HiveTableRelation_ representing acid table to _LogicalRelation_ backed by HiveAcidRelation.

	To use this, initialize SparkSession with the extension builder as mentioned below:

         val spark = SparkSession.builder()
           .appName("Hive-acid-test")
           .config("spark.sql.extensions", "com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension")
           .enableHiveSupport()
           .<OTHER OPTIONS>
           .getOrCreate()

         spark.sql("select * from default.acidtbl")

#### Example

##### Create Hive ACID Table

Drop Existing table

	spark.sql("Drop table if exists aciddemo.t_scala_simple")

Create table

	spark.sql("CREATE TABLE aciddemo.t_scala_simple (status BOOLEAN, tweet ARRAY<STRING>, rank DOUBLE, username STRING) STORED AS ORC TBLPROPERTIES('TRANSACTIONAL' = 'true')")

Check if it is transactional

	spark.sql("DESCRIBE extended aciddemo.t_scala_simple").show()


##### Scala

Read Existing table and insert into acid table

	val df = spark.read.format("HiveAcid").options(Map("table" -> "aciddemo.acidtbl")).load()
	df.write.format("HiveAcid").option("table", "aciddemo.t_scala_simple").mode("append").save()

Read Existing table and insert overwrite acid table

	val df = spark.read.format("HiveAcid").options(Map("table" -> "aciddemo.acidtbl")).load()
	df.write.format("HiveAcid").option("table", "aciddemo.t_scala_simple").mode("overwrite").save()


_Note: User cannot operate directly on file level data as table is required when reading and writing transactionally.
`df.write.format("HiveAcid").mode("overwrite").save("s3n://aciddemo/api/warehouse/aciddemo.db/random")` won't work_

Read acid table

	val df = spark.read.format("HiveAcid").options(Map("table" -> "aciddemo.t_scala_simple")).load()
	df.select("status", "rank").filter($"rank" > "20").show()

##### SQL

Insert into the table select as

	spark.sql("INSERT INTO aciddemo.t_sql_simple select * from aciddemo.acidtbl")

Insert overwrite the table select as

	spark.sql("INSERT OVERWRITE TABLE aciddemo.t_sql_simple select * from aciddemo.acidtbl")

Insert into"

	spark.sql("INSERT INTO aciddemo.t_sql_simple VALUES(false, array("test"), 11.2, 'qubole')")

Read

	spark.sql("SELECT status, rank from aciddemo.t_sql_simple where rank > 20")


## Latest Binaries

ACID datasource is published spark-packages.org. The latest version of the binary is `0.4.2`

## Version Compatibility

### Compatibility with Apache Spark Versions

ACID datasource has been tested to work with Apache Spark 2.4.3, but it should work with older versions as well. However, because of a Hive dependency, this datasource needs Hadoop version 2.8.2 or higher due to [HADOOP-14683](https://jira.apache.org/jira/browse/HADOOP-14683)

_NB: Hive ACID V2 is supported in Hive 3.0.0 onwards and for that hive Metastore db needs to be [upgraded](https://cwiki.apache.org/confluence/display/Hive/Hive+Schema+Tool) to 3.0.0 or above._

### Data Storage Compatibility

1. ACID datasource does not control data storage format and layout, which is managed by Hive. It works with data written by Hive version 3.0.0 and above. Please see [Hive ACID storage layout](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions#HiveTransactions-BasicDesign).

2. ACID datasource works with data stored on local files, HDFS as well as cloud blobstores (AWS S3, Azure Blob Storage etc).

## Developer resources
### Build

1. First, build the dependencies and publish it to local. The *shaded-dependencies* sub-project is an sbt project to create the shaded hive metastore and hive exec jars combined into a fat jar `spark-acid-shaded-dependencies`. This is required due to our dependency on Hive 3 for Hive ACID, and Spark currently only supports Hive 1.2. To compile and publish shaded dependencies jar:

    cd shaded-dependencies
    sbt clean publishLocal

2. Next, build the main project:

	sbt assembly

This will create the `spark-acid-assembly-0.4.2.jar` which can be now used in your application.

### Test
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

1. This datasource when it needs to read data, it talks to the HiveMetaStore Server to get the list of transactions that have been committed, and using that, the list of files it should read from the filesystem (_uses s3 listing_). Given the snapshot of list of file is created by using listing, to avoid inconsistent copy of data, on cloud object store service like S3 guard should be used.

2. This snapshot of list of files is created at the RDD level. These snapshot are at the RDD level so even when using same table in single SQL it may be operating on two different snapshots

    spark.sql("select * from a join a)

3. The files in the snapshot needs to be protected till the RDD is in use. By design concurrent reads and writes on the Hive ACID works with the help of locks, where every client (across multiple engines) that is operating on ACID tables is expected to acquire locks for the duration of reads and writes. The lifetime of RDD can be very long, to avoid blocking other operations like inserts this datasource _DOES NOT_ acquire lock but uses an alternative mechanism to protect reads. Other way the snapshot can be protected is by making sure the files in the snapshot are not deleted while in use. For the current datasoure any table on which Spark is operating `Automatic Compaction` should be disabled. This makes sure that cleaner does not clean any file. To disable automatic compaction on table

         ALTER TABLE <> SET TBLPROPERTIES ("NO_AUTO_COMPACTION"="true")

	When the table is not in use cleaner can be enabled and all the files that needs cleaned will get queued up for cleaner. Disabling compaction do have performance implication on reads/writes as lot of delta file may need to be merged when performing read.

4. Note that even though reads are protected admin operation like `TRUNCATE` `ALTER TABLE DROP COLUMN` and `DROP` have no protection as they clean files with intevention from cleaner. These operations should be performed when Spark is not using the table.


## Contributing

We use [Github Issues](https://github.com/qubole/spark-acid/issues) to track issues.

## Reporting bugs or feature requests

Please use the github issues for the acid-ds project to report issues or raise feature requests.
