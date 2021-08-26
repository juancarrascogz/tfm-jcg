name := "pubsub_spark"

version := (version in ThisBuild).value

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.4.8" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.4.8" % "provided",
  "org.apache.bahir" %% "spark-streaming-pubsub" % "2.2.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}