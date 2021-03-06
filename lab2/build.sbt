lazy val root = (project in file(".")).settings(
  name := "Lab2",
  version := "0.1",
  scalaVersion := "2.11.12",
  organization := "org.sberbank",
  Compile / mainClass := Some("org.sberbank.App"),
  scalacOptions ++= Seq("-language:implicitConversions", "-deprecation"),
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case _                        => MergeStrategy.first
  }
)

val sparkVersion = "2.4.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)