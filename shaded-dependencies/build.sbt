name := "spark-acid-shaded-dependencies"

version := sys.props.getOrElse("package.version", "0.1")

organization:= "com.qubole"

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

publishArtifact in (Compile, packageDoc) := false

publishArtifact in (Compile, packageSrc) := false

publishArtifact in (Compile, packageBin) := false

val hive_version = sys.props.getOrElse("hive.version", "2.1.1-cdh6.2.0")

val orc_version = sys.props.getOrElse("orc.version", "1.5.6")

resolvers += "Additional Maven Repository" at sys.props.getOrElse("hive.repo", "https://repo1.maven.org/maven2/")
resolvers += "cloudera Repository" at sys.props.getOrElse("cloudera", "https://repository.cloudera.com/artifactory/cloudera-repos/")

// Shaded dependency
libraryDependencies ++= Seq(
	// Hive/Orc core dependencies packed.
	"org.apache.hive" % "hive-metastore" % hive_version intransitive(),
	"org.apache.hive" % "hive-exec" % hive_version intransitive(),
	"org.apache.orc" % "orc-core" % orc_version intransitive(),
	"org.apache.orc" % "orc-mapreduce" % orc_version intransitive()

	/*// Only for hive3 client in tests.. but packing it in shaded jars.
	"org.apache.hive" % "hive-jdbc" % hive_version intransitive(),
	"org.apache.hive" % "hive-service" % hive_version intransitive(),
	"org.apache.hive" % "hive-serde" % hive_version intransitive(),
	"org.apache.hive" % "hive-common" % hive_version intransitive(),
	
	// To deal with hive3 metastore library 0.9.3 vs zeppelin thirft
	// library version 0.9.1 conflict when runing Notebooks.
	"org.apache.thrift" % "libfb303" % "0.9.3",
	"org.apache.thrift" % "libthrift" % "0.9.3"*/
)


assemblyShadeRules in assembly := Seq(
	ShadeRule.rename("org.apache.hadoop.hive.**" -> "com.qubole.shaded.hadoop.hive.@1").inAll,
	ShadeRule.rename("org.apache.hive.**" -> "com.qubole.shaded.hive.@1").inAll,
	ShadeRule.rename("org.apache.orc.**" -> "com.qubole.shaded.orc.@1").inAll,
	ShadeRule.rename("org.apache.commons.**" -> "com.qubole.shaded.commons.@1").inAll,
	ShadeRule.rename("org.apache.avro.**" -> "com.qubole.shaded.avro.@1").inAll,
	ShadeRule.rename("org.apache.parquet.**" -> "com.qubole.shaded.parquet.@1").inAll,
	ShadeRule.rename("org.apache.http.**" -> "com.qubole.shaded.http.@1").inAll,
	ShadeRule.rename("org.apache.tez.**" -> "com.qubole.shaded.tez.@1").inAll,

	ShadeRule.rename("com.google.**" -> "com.qubole.shaded.@1").inAll,
	ShadeRule.rename("com.facebook.fb303.**" -> "com.qubole.shaded.facebook.fb303.@1").inAll,
	ShadeRule.rename("org.apache.thrift.**" -> "com.qubole.shaded.thrift.@1").inAll,

	ShadeRule.rename("org.codehaus.jackson.**" -> "com.qubole.shaded.jackson.@1").inAll,
	ShadeRule.rename("org.joda.**" -> "com.qubole.shaded.joda.@1").inAll,
	ShadeRule.rename("org.json.**" -> "com.qubole.shaded.json.@1").inAll,

	ShadeRule.rename("jodd.**" -> "com.qubole.shaded.jodd.@1").inAll,
	ShadeRule.rename("javaewah.**" -> "com.qubole.shaded.javaewah.@1").inAll,
	ShadeRule.rename("io.airlift.**" -> "com.qubole.shaded.io.airlift.@1").inAll,

	ShadeRule.rename("org.openx.data.**" -> "com.qubole.shaded.openx.data.@1").inAll,
	ShadeRule.rename("au.com.bytecode.opencsv.**" -> "com.qubole.shaded.au.com.bytecode.opencsv.@1").inAll,
	ShadeRule.rename("com.readytalk.metrics.**" -> "com.qubole.shaded.readytalk.metrics.@1").inAll
)

import sbtassembly.AssemblyPlugin.autoImport.{ ShadeRule}
import sbtassembly.MergeStrategy
val distinctAndReplace: sbtassembly.MergeStrategy = new sbtassembly.MergeStrategy {
    val name = "distinctAndReplace"
    def apply(tempDir: File, path: String, files: Seq[File]): Either[String, Seq[(File, String)]] = {
        val lines = files flatMap (IO.readLines(_, IO.utf8))
            val unique = lines.distinct
            val replaced = unique.map {  x => x.replace("org.apache.hadoop.hive", "com.qubole.shaded.hadoop.hive")}
            val file = sbtassembly.MergeStrategy.createMergeTarget(tempDir, path)
            IO.writeLines(file, replaced, IO.utf8)
            Right(Seq(file -> path))
    }
}


assemblyMergeStrategy in assembly := {
	// all discarded classes first
	case PathList("javax", xs @ _*) => MergeStrategy.discard
	case PathList("javolution", xs @_*) => MergeStrategy.discard
		// discard non shaded classes in hadoop and qubole packages
	case PathList("org", "apache", "hadoop", xs @_*) => MergeStrategy.discard
	case PathList("org", "apache", "log4j", xs @ _*) => MergeStrategy.last
	case PathList("com", "google", xs @ _*) => MergeStrategy.last
	case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
	case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
	case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
	case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
	case PathList("com","zaxxer", xs @ _*) => MergeStrategy.last
	case PathList("org","apache", "logging", "log4j",  xs @ _*) => MergeStrategy.last
	case PathList("io","netty", xs @ _*) => MergeStrategy.last
	case PathList("org","datanucleus", xs @ _*) => MergeStrategy.last
	case PathList("org", "apache", "arrow", xs @ _*) => MergeStrategy.last
	case PathList("org", "apache", "commons", "lang3", xs @ _*) => MergeStrategy.last
	case PathList("org", "apache", "commons", "lang3", "builder", xs @ _*) => MergeStrategy.last
	case PathList("org", "apache", "commons", "lang3", "concurrent", xs @ _*) => MergeStrategy.last
	case PathList("org", "apache", "commons", "lang3", "event", xs @ _*) => MergeStrategy.last
	case PathList("org", "apache", "commons", "lang3", "exception", xs @ _*) => MergeStrategy.last
	case PathList("org", "apache", "commons", "lang3", "math", xs @ _*) => MergeStrategy.last
	case PathList("org", "apache", "commons", "lang3", "mutable", xs @ _*) => MergeStrategy.last
	case PathList("org", "apache", "commons", "lang3", "reflect", xs @ _*) => MergeStrategy.last
	case PathList("org", "apache", "commons", "lang3", "text", xs @ _*) => MergeStrategy.last
	case PathList("org", "apache", "commons", "lang3", "time", xs @ _*) => MergeStrategy.last
	case PathList("org", "apache", "commons", "lang3", "tuple", xs @ _*) => MergeStrategy.last
	case PathList("com", "qubole", "shaded", "orc", xs @ _*) => MergeStrategy.last
	case PathList("org", "slf4j", "impl", xs @ _*) => MergeStrategy.last
	case PathList("org", "slf4j", "helpers", xs @ _*) => MergeStrategy.last
	case PathList("org", "slf4j", xs @ _*) => MergeStrategy.last

	// discard package.jdo because objects defined inside it are not shaded.
		// So removing for now
	case "package.jdo" => MergeStrategy.discard

	case PathList("META-INF", "services", xs @ _*) => distinctAndReplace
	// case "about.html" => MergeStrategy.rename
	case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
	case "META-INF/mailcap" => MergeStrategy.last
	case "META-INF/mimetypes.default" => MergeStrategy.last
	case "plugin.properties" => MergeStrategy.last
	case "log4j.properties" => MergeStrategy.last
	case "Log4j2Plugins.dat" => MergeStrategy.last
	case "git.properties" => MergeStrategy.last
	case "plugin.xml" => MergeStrategy.last
	case "META-INF/io.netty.versions.properties" => MergeStrategy.last
	case "META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat" => MergeStrategy.last
	case "codegen/config.fmpp" => MergeStrategy.first

	case x =>
			val oldStrategy = (assemblyMergeStrategy in assembly).value
			oldStrategy(x)
}

// do not add scala in fat jar
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// For publishing assembly locally
publishMavenStyle := false

artifact in (Compile, assembly) := {
	val art = (artifact in (Compile, assembly)).value
	art.withClassifier(None)
}

addArtifact(artifact in (Compile, assembly), assembly)

