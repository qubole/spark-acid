/*
 * Copyright 2019 Qubole, Inc.  All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

name := "spark-acid"

organization:= "com.qubole"

/*******************
	* Scala settings
	*/

crossScalaVersions := Seq("2.11.12")

scalaVersion := crossScalaVersions.value.head

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

/**************************
	* Spark package settings
	*/
sparkVersion := sys.props.getOrElse("spark.version", "2.4.0-cdh6.2.0")

spIncludeMaven := true

spIgnoreProvided := true


/************************
	* Library Dependencies
	*/
resolvers += "cloudera Repository" at sys.props.getOrElse("cloudera", "https://repository.cloudera.com/artifactory/cloudera-repos/")
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
		ExclusionRule("org.apache", "hadoop-hdfs")),
	"org.apache.hadoop" % "hadoop-common" % "3.0.0-cdh6.2.0" % "provided",
	"org.apache.hadoop" % "hadoop-hdfs" % "3.0.0-cdh6.2.0" % "provided",
	"org.apache.commons" % "commons-lang3" % "3.7" % "provided",
	// antlr-runtime
	"org.antlr" % "antlr4-runtime" % "4.7" % "provided"
)

lazy val scalatest = "org.scalatest" %% "scalatest" % "3.0.5"

// Dependencies for Test
libraryDependencies ++= Seq(
	"org.apache.hadoop" % "hadoop-common" % "3.0.0-cdh6.2.0" % "provided",
	"org.apache.hadoop" % "hadoop-hdfs" % "3.0.0-cdh6.2.0" % "provided",
	"org.apache.commons" % "commons-lang3" % "3.7" % "provided",
	// Dependencies for tests
	//
	"org.scalatest" %% "scalatest" % "3.0.5" % "test",
	"junit" % "junit" % "4.12" % "it,test",
	"com.novocode" % "junit-interface" % "0.11" % "it,test",
	"org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test" classifier "tests",
	"org.apache.spark" %% "spark-core" % sparkVersion.value % "test" classifier "tests",
	"org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" classifier "tests"
)

// Shaded jar dependency
libraryDependencies ++= Seq(
	// intransitive() because we don't want to include any transitive dependencies of shaded-dependencies jar in main jar
	// ideally all such dependencies should be shaded inside shaded-dependencies jar
	"com.qubole" %% "spark-acid-shaded-dependencies" % sys.props.getOrElse("package.version", "0.1") intransitive()
)

/**************************************
	* Remove Shaded Depenedency from POM
	*/

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
	"org.apache.orc" % "orc-mapreduce",

	"org.slf4j" % "slf4j-api"
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
                <id>amoghmargoor</id>
                <name>Amogh Margoor</name>
                <url>https://github.com/amoghmargoor</url>
	    </developer>
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
	    <developer>
                <id>sourabh912</id>
                <name>Sourabh Goyal</name>
                <url>https://github.com/sourabh912</url>
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

/**
	* Antlr settings
	*/
antlr4Settings
antlr4PackageName in Antlr4 := Some("com.qubole.spark.datasources.hiveacid.sql.catalyst.parser")
antlr4GenListener in Antlr4 := true
antlr4GenVisitor in Antlr4 := true
antlr4Version := "4.7.2"


/*******************
	*  Test settings
	*/

parallelExecution in IntegrationTest := false

// do not run test at assembly
test in assembly := {}

// do not add scala in fat jar
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

//Integration test
lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    libraryDependencies += scalatest % "it"
  )

// exclude antlr classes from assembly since those
// are available in spark at runtime
// any other classes to be excluded from assembly
// should be added here
assemblyExcludedJars in assembly := {
	val cp = (fullClasspath in assembly).value
	cp filter {_.data.getName.contains("antlr")}
}

/***********************
	* Release settings
	*/

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
	pushChanges
)
