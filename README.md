The repository for acid datasource (in development)

This has the following sbt projects:

1. acid-datasource: The main project for the datasource. This has the actual code for the datasource
2. hive-shaded-exec-metastore: This is an sbt projec to create the shaded hive metastore and hive exec jars combined into a fat jar(hive-shaded-exec-metastore-assembly-0.1.jar referred below). This jar has been created already and packaged into the lib folder of acid-datasource as an umnanaged dependency 



To use the datasource:

1. Go to the acid-datasource directory
2. sbt package. This will create the acid-datasource_2.11-0.1.jar jar
3.


spark-shell --jars /Users/asomani/src/acid-ds/acid-datasource/target/scala-2.11/acid-datasource_2.11-0.1.jar,/Users/asomani/src/acid-ds/acid-datasource/lib/hive-shaded-exec-metastore-assembly-0.1.jar.

scala> var df = spark.read.format("com.qubole.spark.HiveAcidDataSource").options(Map("table" -> "default.newtest_hello_mm")).load()
df.collect()




Notes:

1. This will work only with an external HMS, even in spark local mode.
