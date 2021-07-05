
resolvers += "spark-packages" at sys.props.getOrElse("spark.repo", "https://repos.spark-packages.org/")

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.6")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.11")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")

addSbtPlugin("org.foundweekends" % "sbt-bintray" % "0.5.4")

addSbtPlugin("com.simplytyped" % "sbt-antlr4" % "0.7.13")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")
