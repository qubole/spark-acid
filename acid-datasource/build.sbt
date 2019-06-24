name := "acid-datasource"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.3" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.3" % "provided"

