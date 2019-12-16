name := "spark-acid"

organization:= "com.qubole"

crossScalaVersions := Seq("2.11.12")

scalaVersion := crossScalaVersions.value.head

sparkVersion := sys.props.getOrElse("spark.version", "2.4.3")

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

resolvers += "spark-packages" at sys.props.getOrElse("spark.repo", "https://dl.bintray.com/spark-packages/maven/")

libraryDependencies ++= Seq(
	// Adding test classifier seems to break transitive resolution of the core dependencies
	"org.apache.spark" %% "spark-hive" % sparkVersion.value % "provided" excludeAll(
		ExclusionRule("org.apache", "hadoop-common"),
		ExclusionRule("org.apache", "hadoop-hdfs")),
	"org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided" excludeAll(
		ExclusionRule("org.apache", "hadoop-common"),
		ExclusionRule("org.apache", "hadoop-hdfs")),
	"org.apache.spark" %% "spark-core" % sparkVersion.value % "provided" excludeAll(
		ExclusionRule("org.apache", "hadoop-common"),
		ExclusionRule("org.apache", "hadoop-hdfs")),
	"org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "provided" excludeAll(
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
	"com.qubole" %% "spark-acid-shaded-dependencies" % sys.props.getOrElse("package.version", "0.1")
)


// Remove shaded dependency jar from pom.
import scala.xml.{Node => XmlNode, NodeSeq => XmlNodeSeq, _}
import scala.xml.transform.{RewriteRule, RuleTransformer}

pomPostProcess := { (node: XmlNode) =>
  new RuleTransformer(new RewriteRule {
    override def transform(node: XmlNode): XmlNodeSeq = node match {
      case e: Elem if e.label == "dependency" && e.child.filter(_.label == "groupId").text.mkString == "com.qubole" =>
        val organization = e.child.filter(_.label == "groupId").flatMap(_.text).mkString
        val artifact = e.child.filter(_.label == "artifactId").flatMap(_.text).mkString
        val version = e.child.filter(_.label == "version").flatMap(_.text).mkString
        Comment(s"dependency $organization#$artifact;$version has been omitted")
      case _ => node
    }
  }).transform(node).head
}

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

// Spark Package Section
spName := "qubole/spark-acid"

spShade := true

spAppendScalaVersion := true

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

pomExtra :=
    <url>https://github.com/qubole/spark-acid</url>
        <scm>
            <url>git@github.com:qubole/spark-acid.git</url>
            <connection>scm:git:git@github.com:qubole/spark-acid.git</connection>
        </scm>
        <developers>
            <developer>
                <id>citrusraj</id>
                <name>Rajkumar Iyer</name>
                <url>https://github.com/citrusraj</url>
            </developer>
            <developer>
                <id>somani</id>
                <name>Abhishek Somani</name>
                <url>https://github.com/somani</url>
            </developer>
            <developer>
                <id>prakharjain09</id>
                <name>Prakhar Jain</name>
                <url>https://github.com/prakharjain09</url>
            </developer>
        </developers>


publishMavenStyle := true

bintrayReleaseOnPublish := false

import ReleaseTransformations._

// Add publishing to spark packages as another step.
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  pushChanges,
  releaseStepTask(spDist),
  releaseStepTask(spPublish)
)
