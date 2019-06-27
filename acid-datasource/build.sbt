name := "hiveacid-datasource"

organization := "com.qubole"

version := "0.1"

scalaVersion := "2.11.12"

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

// do not run test at assembly
test in assembly := {}

// For publishing assembly
artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}
addArtifact(artifact in (Compile, assembly), assembly)

libraryDependencies ++= Seq(
  // Adding test classifier seems to break transitive resolution of the core dependencies
  "org.apache.spark" %% "spark-hive" % "2.4.3" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided",
  "org.apache.spark" %% "spark-core" % "2.4.3" % "provided",
  "org.apache.spark" %% "spark-catalyst" % "2.4.3" % "provided",

  "com.qubole" %% "spark-hiveacid-shaded-dependencies" % "0.1",
  // Dependencies for tests
  //
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
)

excludeDependencies ++=Seq(
	// hive
	ExclusionRule("org.apache.hive", "hive-exec"),
	ExclusionRule("org.apache.hive", "hive-metastore"),
	ExclusionRule("org.apache.hive", "hive-jdbc"),
	ExclusionRule("org.apache.hive", "hive-service"),
	ExclusionRule("org.apache.hive", "hive-serde"),
	ExclusionRule("org.apache.hive", "hive-common"),
	
	// orc
	ExclusionRule("org.apache.orc", "orc-core"),
)
