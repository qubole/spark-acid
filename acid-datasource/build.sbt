name := "acid_datasource"

organization:= "com.qubole"

scalaVersion := "2.11.12"

sparkVersion := "2.4.3"

scalacOptions ++= Seq(
	"-Xlint",
	"-Xfatal-warnings",
	"-deprecation",
	"-unchecked",
	"-optimise",
	"-Yinline-warnings"
)

scalacOptions in (Compile, doc) ++= Seq(
	"-no-link-warnings" // Suppresses problems with Scaladoc @throws links
)

resolvers += "spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies ++= Seq(
	// Adding test classifier seems to break transitive resolution of the core dependencies
	"org.apache.spark" %% "spark-hive" % "2.4.3" % "provided" excludeAll(
		ExclusionRule("org.apache", "hadoop-common"),
		ExclusionRule("org.apache", "hadoop-hdfs")),
	"org.apache.spark" %% "spark-sql" % "2.4.3" % "provided" excludeAll(
		ExclusionRule("org.apache", "hadoop-common"),
		ExclusionRule("org.apache", "hadoop-hdfs")),
	"org.apache.spark" %% "spark-core" % "2.4.3" % "provided" excludeAll(
		ExclusionRule("org.apache", "hadoop-common"),
		ExclusionRule("org.apache", "hadoop-hdfs")),
	"org.apache.spark" %% "spark-catalyst" % "2.4.3" % "provided" excludeAll(
		ExclusionRule("org.apache", "hadoop-common"),
		ExclusionRule("org.apache", "hadoop-hdfs"))
)

libraryDependencies ++= Seq(
	"org.apache.hadoop" % "hadoop-common" % "2.8.1" % "provided",
	"org.apache.hadoop" % "hadoop-hdfs" % "2.8.1" % "provided",
	// Dependencies for tests
	//
	"org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

// Shaded jar dependency
libraryDependencies ++= Seq(
	"com.qubole" %% "spark-acid-shaded-dependencies" % "0.1"
)

excludeDependencies ++= Seq (
	// hive
	"org.apache.hive" % "hive-exec",
	"org.apache.hive" % "hive-metastore",
	"org.apache.hive" % "hive-jdbc",
	"org.apache.hive" % "hive-service",
	"org.apache.hive" % "hive-serde",
	"org.apache.hive" % "hive-common",

	// orc
	"org.apache.orc" % "orc-core",
	"org.apache.orc" % "orc-mapreduce"
)

// do not run test at assembly
test in assembly := {}

skip in publish := true
