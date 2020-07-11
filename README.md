# Hive ACID Data Source for Apache Spark

A Datasource on top of Spark Datasource V1 APIs, that provides Spark support for [Hive ACID transactions](https://cwiki.apache.org/confluence/display/Hive/Hive+Transactions).

This datasource provides the capability to work with Hive ACID V2 tables, both Full ACID tables as well as Insert-Only tables.

functionality availability matrix

Functionality | Full ACID table | Insert Only Table |
------------- | --------------- | ----------------- |
READ | ``>= v0.4.0`` | ``>= v0.4.0`` |
INSERT INTO / OVERWRITE | ``>= v0.4.3`` |  ``>= v0.4.4`` |
CTAS | ``>= v0.4.3`` |  ``>= v0.4.4`` |
UPDATE | ``>= v0.5.0`` |  Not Supported |
DELETE | ``>= v0.5.0`` |  Not Supported |
MERGE | ``> v0.5.0`` | Not Supported |
STREAMING INSERT | ``>= v0.5.0`` | ``>= v0.5.0`` |

*Note: In case of insert only table for support of write operation compatibility check needs to be disabled*

## Quick Start

- [QuickStart](#quickstart)
    - [Prerequisite](#prerequisite)
    - [Config](#config)
    - [Run](#run)
- [Usage](#usage)
    - [Read ACID Table](#read-acid-table)
    - [Batch Write into ACID Table](#batch-write-into-acid-table)
    - [Stream Write into ACID Table](#stream-write-into-acid-table)
    - [Update](#updates)
    - [Delete](#deletes)
    - [Merge](#merge)
- [Version Compatibility](#version-compatibility)
- [Developer Resources](#developer-resources)
- [Design Consideration](#design-constraints)
- [Contributing](#contributing)
- [Report Bugs](#reporting-bugs-or-feature-requests)
- [Known Issues](#known-issues)
    

## QuickStart

### Prerequisite
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

       `spark-shell --packages qubole:spark-acid:0.5.0-s_2.11

2. If you built the jar yourself, copy the `spark-acid-assembly-0.5.0.jar` jar into `$SPARK_HOME/assembly/target/scala.2_11/jars` and run

       spark-shell

#### DataFrame API

To operate on Hive ACID table from Scala / pySpark, the table can be directly accessed using this datasource. Note the short name of this datasource is `HiveAcid`. Hive ACID table are tables in HiveMetastore so any operation of read and/or write needs `format("HiveAcid").option("table", "<table name>"")`. _Direct read and write from the file is not supported_

    scala> val df = spark.read.format("HiveAcid").options(Map("table" -> "default.acidtbl")).load()
    scala> df.collect()

#### SQL

To read an existing Hive acid table through pure SQL, there are two ways:

1. Use SparkSession extensions framework to add a new Analyzer rule (HiveAcidAutoConvert) to Spark Analyser. This analyzer rule automatically converts an _HiveTableRelation_ representing acid table to _LogicalRelation_ backed by HiveAcidRelation.
   
   	To use this, initialize SparkSession with the extension builder as mentioned below:
   
            val spark = SparkSession.builder()
              .appName("Hive-acid-test")
              .config("spark.sql.extensions", "com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension")
              .enableHiveSupport()
              .<OTHER OPTIONS>
              .getOrCreate()
   
            spark.sql("select * from default.acidtbl")

2. Create a dummy table that acts as a symlink to the original acid table. This symlink is required to instruct Spark to use this datasource against an existing table.

	To create the symlink table:

		spark.sql("create table symlinkacidtable using HiveAcid options ('table' 'default.acidtbl')")

        spark.sql("select * from symlinkacidtable")


   _Note: This will produce a warning indicating that Hive does not understand this format_

     WARN hive.HiveExternalCatalog: Couldn’t find corresponding Hive SerDe for data source provider com.qubole.spark.hiveacid.datasource.HiveAcidDataSource. Persisting data source table `default`.`sparkacidtbl` into Hive metastore in Spark SQL specific format, which is NOT compatible with Hive.

   _Please ignore it, as this is a sym table for Spark to operate with and no underlying storage._

## Usage

This section talks about major functionality provided by the data source and example code snippets for them.

### Create/Drop ACID Table
#### SQL Syntax
Same as CREATE and DROP supported by Spark SQL.

##### Examples

Drop Existing table

	spark.sql("DROP TABLE if exists acid.acidtbl")

Create table

	spark.sql("CREATE TABLE acid.acidtbl (status BOOLEAN, tweet ARRAY<STRING>, rank DOUBLE, username STRING) STORED AS ORC TBLPROPERTIES('TRANSACTIONAL' = 'true')")

_Note: Table property ``'TRANSACTIONAL' = 'true'`` is required to create ACID table_

Check if it is transactional

	spark.sql("DESCRIBE extended acid.acidtbl").show()

### Read ACID Table

#### DataFrame API
Read acid table

	val df = spark.read.format("HiveAcid").options(Map("table" -> "acid.acidtbl")).load()
	df.select("status", "rank").filter($"rank" > "20").show()

Read acid via implicit API
    
    import com.qubole.spark.hiveacid._
    
    val df = spark.read.hiveacid("acid.acidtbl")
    df.select("status", "rank").filter($"rank" > "20").show()

#### SQL SYNTAX
Same as SELECT supported by Spark SQL.

##### Example

    spark.sql("SELECT status, rank from acid.acidtbl where rank > 20")

_Note: ``com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension`` has to be added to ``spark.sql.extensions`` for above._

### Batch Write into ACID Table

#### DataFrame API

Insert into

	val df = spark.read.parquet("tbldata.parquet")
	df.write.format("HiveAcid").option("table", "acid.acidtbl").mode("append").save()

Insert overwrite

	val df = spark.read.parquet("tbldata.parquet")
	df.write.format("HiveAcid").option("table", "acid.acidtbl").mode("overwrite").save()

Insert into using implicit API

    import com.qubole.spark.hiveacid._
    
    val df = spark.read.parquet("tbldata.parquet")
    df.write.hiveacid("acid.acidtbl", "append")

#### SQL Syntax
Same as INSERT supported by Spark SQL

##### Example
Insert into the table select as

	spark.sql("INSERT INTO acid.acidtbl select * from sample_data")

Insert overwrite the table select as

	spark.sql("INSERT OVERWRITE TABLE acid.acidtbl select * from sample_data")

Insert into"

	spark.sql("INSERT INTO acid.acidtbl VALUES(false, array("test"), 11.2, 'qubole')")

_Note: ``com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension`` has to be added to ``spark.sql.extensions`` for above SQL statements._
### Stream Write into ACID Table
ACID table supports streaming writes and can also be used as a Streaming Sink. 
Streaming write happens under transactional guarantees which allows other
concurrent writes to the same table either streaming writes or batch writes.
For exactly-once semantics, ``acid.streaming.metadataDir`` is specified to 
store the latest batchId processed. Note, that concurrent streaming writes 
to the same table should have different metadataDir specified.

    val query = newDf
      .writeStream
      .format("HiveAcid")
      .options(Map(
        "table" ->"acid.acidtbl",
        "acid.streaming.metadataDir"->"/tmp/_acid_streaming/_query_2"))
      .outputMode(OutputMode.Append)
      .option("checkpointLocation", "/tmp/attempt16")
      .start()

### Updates

#### SQL Syntax

    UPDATE tablename SET column = updateExp [, column = updateExp ...] [WHERE expression]
    
* ``column`` must be a column of the table being updated.
* ``updateExp`` is an expression that Spark supports in the SELECT clause. Subqueries are not supported.
* ``WHERE`` clause specifies the row to be updated.
* Partitioning columns cannot be updated.
* Bucketed table are not supported currently.

#### Example
    
    spark.sql("UPDATE acid.acidtbl set rank = rank - 1, status = true where rank > 20 and rank < 25 and status = false")

_Note: ``com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension`` has to be added to ``spark.sql.extensions`` for above._
### Deletes

#### SQL syntax
    DELETE FROM tablename [WHERE expression]

* ``WHERE`` clause specifies rows to be deleted from ``tablename``.
* Bucketed tables are not supported currently.

##### Example

    DELETE from acid.acidtbl where rank = 1000

_Note: ``com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension`` has to be added to ``spark.sql.extensions`` for above._

### Merge

#### SQL syntax
    MERGE INTO <target table> [AS T] USING <source table> [AS S]
    ON <boolean merge expression>
    WHEN MATCHED [AND <boolean expression1>] THEN <match_clause>
    WHEN MATCHED [AND <boolean expression2>] THEN <match_clause>
    WHEN NOT MATCHED [AND <boolean expression3>] THEN INSERT VALUES ( <insert value list> )

    <match_clause> ::
                        UPDATE SET <set clause list>
                        DELETE
    
    <insert value list> :: 
                        value 1 [, value 2, value 3, ...]
                        [value 1, value 2, ...] * [, value n, value n+1, ...]
    
    <update set list> ::
                        target_col1 = value 1 [, target_col2 = value 2 ...]
                    

* ``<target table>`` needs to be Full ACID table. ``T`` is optional placeholder for target alias. 
* ``<source table>`` needs to be a table defined. You can use functions like ``createOrReplaceTempView`` to store source DataFrames as tables.
``S`` is optional placeholder for source alias.
* ``<merge expression>`` are the join expressions used as merge condition
* Match clauses (UPDATE and DELETE)
    * At most 2 match clauses are allowed i.e., minimum 0 and maximum 2.
    * Only UPDATE and DELETE operations are supported in match clause.
    * ``<boolean expression1> `` and ``<boolean expression2>`` are optional match conditions. 
    * If 2 match clauses are specified:
        * Both should be different operations.
        * First match clause should have a match condition.
        * If a target row qualifies for both match clauses as their match conditions overlap, 
          then only the first clause will be executed on them.
* INSERT clause 
    * is only supported for non-matched clause.
    * supports ``*`` to be used anywhere in value list and it resolves into source table columns.
    * values to be inserted should exactly match the number of target columns after ``*`` resolution and 
      also match corresponding data type.
* Cardinality Check: SQL standard enforces that one row of target doesn't match multiple rows of source.
  This check is enforced and runtime exception is thrown if it is violated.

#### Example

    MERGE INTO target as t USING source as s
    ON t.id = s.id
    WHEN MATCHED AND t.city = 'Bangalore' THEN UPDATE t.city = s.city
    WHEN MATCHED AND t.dept = 'closed' THEN DELETE
    WHEN NOT MATCHED AND t.city = ('Bangalore', 'San Jose') THEN INSERT VALUES (*, '07', '2020')

#### Performance consideration
* MERGE operation is a pretty loaded statement and can be expensive in nature.
* MERGE operation will perform ``Right Outer Join`` between target and source. 
  This can lead to full table scan of the target table. 
  Please consider partitioning the target table and only mentioning required partitions in merge condition for MERGE operations.
* When only INSERT clause is present, ``Left Anti Join`` between source and target will be performed. It will be 
  cheaper than ``Right Outer Join`` between target and source.
* Cardinality check (as described above) also requires Join. We reuse the same ``Right Outer Join`` done for MERGE operation 
  to avoid extra Join for this check. When only INSERT clause is present this check is not done as it is not required.

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

This will create the `spark-acid-assembly-0.5.0.jar` which can be now used in your application.

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

1. You can join the group for queries and discussions by sending email to: spark-acid+subscribe@googlegroups.com
   On subscribing, you will be sent email to confirm joining the group.

2. We use [Github Issues](https://github.com/qubole/spark-acid/issues) to track issues.
   Please feel free to open an issue for any queries, bugs and feature requests.

3. Pull Request can be raised against any open issues and are most welcome. 
   Processes or guidelines for the same is not formal currently.

## Reporting bugs or feature requests

Please use the github issues for the acid-ds project to report issues or raise feature requests. 
You can also join this group to discuss them: spark-acid+subscribe@googlegroups.com

## Known Issues

1. Insert in static partitions is not supported via spark acid. For example query like "insert into tbl partition (p1=1) ...." will not work. It is because spark currently does not support partitioned datasources. It only supports partitions in Hive table relation or a file based relation. But spark acid relation is neither of them.
2. Because of an open source issue [HIVE-21052](https://issues.apache.org/jira/browse/HIVE-21052), users started hitting the issue described by [@amoghmargoor](https://github.com/amoghmargoor) in [this](https://issues.apache.org/jira/browse/HIVE-21052?focusedCommentId=17152785&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-17152785) comment for **partitioned** tables.
The workaround of the issue HIVE-21052 is that we don't set dynamic partition flag when making lock request in spark acid. HIVE-21052 will **not** have any impact on the functionality and it is expected to continue to work well. However users will have to do some manual cleanups. They are not serious in nature and can be performed once in a while. These are:

- If transaction is successful
   - For every transaction on partitioned table, now a null entry gets created in COMPLETED_TXN_COMPONENTS table when a transaction is moved from TXN_COMPONENTS. This entry does not get removed after compaction. Note that there would be only one such null entry in COMPLETED_TXN_COMPONENTS table for a transaction. For example if your transaction has touched 100 partitions only one entry of null partitions would get created. So the null entries should not overwhelm the table.
   - To delete this null entry from COMPLETED_TXN_COMPONENTS table, manually run this query once in a while on metastore db. Note that this is only applicable on partitioned tables
   
       ``DELETE FROM completed_txn_components WHERE ctc_partition IS NULL AND ctc_writeid IS NULL  AND ctc_table = <TABLE_NAME>``
                                                    
- If transaction is aborted

   - Transaction now remains in TXN_COMPONENTS and TXNS tables. Any future reads are not impacted by this aborted transaction though.
   - The cleanup involves 2 simple steps to be followed for partitioned tables in the following order. These are:
      1. Delete files in the object store which were written by the aborted transaction. To find out the write id the simple sql query is: 
           
            ``SELECT t.TXN_ID,  T2W_WRITEID as WRITE_ID from TXNS as t JOIN TXN_COMPONENTS as tc ON tc.TC_TXNID = t.TXN_ID JOIN TXN_TO_WRITE_ID as tw on t.TXN_ID = tw.T2W_TXNID and t.TXN_STATE = 'a' and tc.TC_PARTITION is NULL``
            
            For example if your write ID is 4, then you will need to cleanup  all delta/delta_delete/base directories with name: delta_0000004_0000004/delete_delta_0000004_0000004/base_0000004
      2. Delete entry from TXN_COMPONENTS table for aborted transaction. Complete sql query looks like
      
          ``WITH aborted_transactions AS (
            SELECT
                t.txn_id,  
                tw.t2w_writeid AS write_id
            FROM
                txns AS t 
                JOIN txn_components AS tc 
                    ON t.txn_id = tc.tc_txnid
                JOIN txn_to_write_id AS tw 
                    ON t.txn_id = tw.t2w_txnid
                    AND t.txn_state = 'a' 
                    AND tc.tc_partition IS NULL
            )
            DELETE FROM txn_components WHERE tc_txnid IN (SELECT txn_id FROM aborted_transactions)`` 
         