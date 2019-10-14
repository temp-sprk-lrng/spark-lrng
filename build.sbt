name := "spark-lrng"

version := "0.1"

scalaVersion := "2.11.0"
val sparkVersion = "2.4.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
libraryDependencies += "org.apache.commons" % "commons-email" % "1.3.1"
libraryDependencies += "org.jsoup" % "jsoup" % "1.7.2"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.7.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
