name := "acid-datasource"

version := "0.1"

scalaVersion := "2.11.12"

//assemblyShadeRules in assembly := Seq(
//  ShadeRule.rename("org.apache.hadoop.hive.**" -> "com.qubole.spark.hive.@1").inAll
//)

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.3" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.3" % "provided"

//libraryDependencies += "org.apache.hive" % "hive-metastore" % "3.1.1"


//assemblyMergeStrategy in assembly := {
//  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
//  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
//  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
//  case PathList("javax", "jdo", xs @ _*) => MergeStrategy.last
//  case PathList("org", "apache", "log4j", xs @ _*) => MergeStrategy.last
//  case PathList("com", "google", xs @ _*) => MergeStrategy.last
//  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
//  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
//  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
//  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
//  case PathList("com","zaxxer", xs @ _*) => MergeStrategy.last
//  case PathList("org","apache", "logging", "log4j",  xs @ _*) => MergeStrategy.last
//  case PathList("io","netty", xs @ _*) => MergeStrategy.last
//  case PathList("org","datanucleus", xs @ _*) => MergeStrategy.last
//  case PathList("org", "apache", "arrow", xs @ _*) => MergeStrategy.last
//  case PathList("org", "slf4j", "impl", xs @ _*) => MergeStrategy.last
//  //case "about.html" => MergeStrategy.rename
//  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
//  case "META-INF/mailcap" => MergeStrategy.last
//  case "META-INF/mimetypes.default" => MergeStrategy.last
//  case "plugin.properties" => MergeStrategy.last
//  case "log4j.properties" => MergeStrategy.last
//  case "Log4j2Plugins.dat" => MergeStrategy.last
//  case "git.properties" => MergeStrategy.last
//  case "plugin.xml" => MergeStrategy.last
//  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
//  case "META-INF/org/apache/logging/log4j/core/config/plugins/Log4j2Plugins.dat" => MergeStrategy.last
//  case x =>
//    val oldStrategy = (assemblyMergeStrategy in assembly).value
//    oldStrategy(x)
//}
