name := "spark-acid"

organization in ThisBuild := "com.qubole"

scalaVersion in ThisBuild := "2.11.12"

sparkVersion in ThisBuild := "2.4.3"

spName in ThisBuild := s"qubole/spark-acid"

publishMavenStyle := true

spAppendScalaVersion := true

spShade := true

// Redirection to publish Fat Jar.
lazy val acid_datasource = project in file("acid-datasource")
assembly in spPackage := (assembly in acid_datasource).value

credentials in ThisBuild += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

licenses in ThisBuild += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

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


bintrayReleaseOnPublish in ThisBuild := false

import ReleaseTransformations._

// Add publishing to spark packages as another step.
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
//runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  releaseStepTask(spPublish)
)
