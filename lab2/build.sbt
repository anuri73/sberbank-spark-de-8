lazy val root = (project in file(".")).settings(
  name := "Lab2",
  version := "0.1",
  scalaVersion := "2.12.10",
  organization := "org.sberbank",
  Compile / mainClass := Some("org.sberbank.App"),
  scalacOptions ++= Seq("-language:implicitConversions", "-deprecation"),
  crossScalaVersions := Seq("2.11.12", "2.12.10"),
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case _                        => MergeStrategy.first
  }
)

val sparkVersion = "2.4.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.lihaoyi" %% "upickle" % "0.7.1",
  "org.scalameta" %% "munit" % "0.7.29" % Test
)

testFrameworks += new TestFramework("munit.Framework")
