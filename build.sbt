import sbtassembly.AssemblyPlugin.autoImport.ShadeRule

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

val publishVersion = "0.1-SNAPSHOT"
val orgPrefix = "com.qubole"

lazy val commonSettings = Seq(
    organization := orgPrefix,
    scalaVersion := "2.11.12",

    // Published snapshot
    version := publishVersion,
    isSnapshot := true,

    scalacOptions ++= Seq(
        "-Xlint",
        "-Xfatal-warnings",
        "-deprecation",
        "-unchecked",
        "-optimise",
        "-Yinline-warnings"
    ),

    scalacOptions in (Compile, doc) ++= Seq(
        "-no-link-warnings" // Suppresses problems with Scaladoc @throws links
    ),

    // do not run test at assembly
    test in assembly := {},

)

lazy val commonShadeSettings = Seq(
    assemblyShadeRules in assembly := Seq(
        ShadeRule.rename("org.apache.hadoop.hive.**" -> "com.qubole.shaded.hadoop.hive.@1").inAll,
        ShadeRule.rename("org.apache.hive.**" -> "com.qubole.shaded.hive.@1").inAll,
        ShadeRule.rename("org.apache.orc.**" -> "com.qubole.shaded.orc.@1").inAll,
        ShadeRule.rename("com.google.**" -> "com.qubole.shaded.@1").inAll,
    ),

    assemblyMergeStrategy in assembly := {
        case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
        case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
        case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
        case PathList("javax", "jdo", xs @ _*) => MergeStrategy.last
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
        case PathList("META-INF", "services", xs @ _*) => distinctAndReplace
            //case "about.html" => MergeStrategy.rename
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
    },

    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),

	publishMavenStyle := false
)

lazy val commonHiveAcidSettings = Seq(
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
            ExclusionRule("org.apache", "hadoop-hdfs")),

        "org.apache.hadoop" % "hadoop-common" % "2.8.1" % "provided",
        "org.apache.hadoop" % "hadoop-hdfs" % "2.8.1" % "provided",

        // Dependencies for tests
        //
        "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    ),

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
        ExclusionRule("org.apache.orc", "orc-mapreduce"),
    )

)

val oss_hive_version = "3.1.1"
val oss_orc_version = "1.5.6"
val dependencies_jar_name = "spark-hiveacid-shaded-dependencies"

// Shaded dependency
lazy val shaded_dependencies = project
    .settings(
        // Published Name
        name := dependencies_jar_name,

        commonSettings,
        commonShadeSettings,

        libraryDependencies ++= Seq(
            // Hive/Orc core dependencies packed.
            "org.apache.hive" % "hive-metastore" % oss_hive_version intransitive(),
            "org.apache.hive" % "hive-exec" % oss_hive_version intransitive(),
            "org.apache.orc" % "orc-core" % oss_orc_version intransitive(),
            "org.apache.orc" % "orc-mapreduce" % oss_orc_version intransitive(),

            // Only for hive3 client in tests.. but packing it in shaded jars.
            "org.apache.hive" % "hive-jdbc" % oss_hive_version intransitive(),
            "org.apache.hive" % "hive-service" % oss_hive_version intransitive(),
            "org.apache.hive" % "hive-serde" % oss_hive_version intransitive(),
            "org.apache.hive" % "hive-common" % oss_hive_version intransitive(),
        ),

		// For publishing
		artifact in (Compile, assembly) := {
			val art = (artifact in (Compile, assembly)).value
				art.withClassifier(Some("assembly"))
		},
        addArtifact(artifact in (Compile, assembly), assembly)
    )

lazy val acid_datasource = (project in file("acid-datasource"))
    .settings(
        name := "spark-hiveacid-datasource",
        commonSettings,
        commonHiveAcidSettings,
        libraryDependencies ++= Seq(
            orgPrefix %% dependencies_jar_name % publishVersion,
        ),
		// For publishing
		artifact in (Compile, assembly) := {
			val art = (artifact in (Compile, assembly)).value
			art.withClassifier(Some("assembly"))
		},
        addArtifact(artifact in (Compile, assembly), assembly)
    )
