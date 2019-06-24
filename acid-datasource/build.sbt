name := "acid-datasource"

organization := "com.qubole"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  // Adding test classifier seems to break transitive resolution of the core dependencies
  "org.apache.spark" %% "spark-hive" % "2.4.3" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided",
  "org.apache.spark" %% "spark-core" % "2.4.3" % "provided",
  "org.apache.spark" %% "spark-catalyst" % "2.4.3" % "provided",

  // Test deps
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.spark" %% "spark-catalyst" % "2.4.3" % "test" classifier "tests",
  "org.apache.spark" %% "spark-core" % "2.4.3" % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % "2.4.3" % "test" classifier "tests"
)
