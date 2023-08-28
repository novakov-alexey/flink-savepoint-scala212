Global / onChangedBuildSource := ReloadOnSourceChanges

ThisBuild / organization := "com.github.novakov-alexey"
ThisBuild / scalaVersion := "2.12.7"

lazy val flinkVersion = "1.15.2" // "1.14.6"

lazy val root = (project in file(".")).settings(
  name := "scala -212-savepoint",
  libraryDependencies ++= Seq(
    "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
    "org.apache.flink" % "flink-state-processor-api" % flinkVersion % Provided,
    "org.apache.flink" % "flink-clients" % flinkVersion % Provided
  ),
  javaOptions ++= Seq("-Dsun.io.serialization.extendedDebugInfo=true")
)

// make run command include the provided dependencies
Compile / run := Defaults
  .runTask(
    Compile / fullClasspath,
    Compile / run / mainClass,
    Compile / run / runner
  )
  .evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true
