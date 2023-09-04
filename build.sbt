Global / onChangedBuildSource := ReloadOnSourceChanges

ThisBuild / organization := "com.github.novakov-alexey"
ThisBuild / scalaVersion := "2.12.7"

 lazy val flinkVersion = "1.15.2"
//lazy val flinkVersion = "1.14.6"

lazy val root = (project in file(".")).settings(
  name := "scala-212-savepoint",
  libraryDependencies ++= Seq(
    "org.apache.flink" % "flink-streaming-java" % flinkVersion % Provided,
    //"org.apache.flink" %% "flink-streaming-scala" % flinkVersion % Provided,
    "org.apache.flink" % "flink-state-processor-api" % flinkVersion % Provided,
    "org.apache.flink" % "flink-clients" % flinkVersion % Provided,
    "org.apache.flink" % "flink-avro" % flinkVersion,
    "org.apache.avro" % "avro" % "1.11.2"
  ),
  javaOptions ++= Seq("-Dsun.io.serialization.extendedDebugInfo=true"),
  ThisBuild / assemblyMergeStrategy := {
    case PathList(ps @ _*) if ps.last endsWith "module-info.class" =>
      MergeStrategy.first
    case "module-info.class" => MergeStrategy.first
    case x =>
      val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
      oldStrategy(x)
  },
  assembly / assemblyExcludedJars := {
    val cp = (assembly / fullClasspath).value
    cp filter { f =>
      Set(
        "flink-shaded-zookeeper-3-3.5.9-15.0.jar"
        // "jackson-databind-2.14.2.jar"
      ).contains(
        f.data.getName
      )
    }
  }
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
Compile / sourceGenerators += (Compile / avroScalaGenerateSpecific).taskValue
Global / cancelable := true
