# Example of stateful job using Apache Flink Scala API 2.12 

To build a fat-jar run:

```bash
sbt assembly
```

in the result you will a JAR file at target/scala-2.12/scala-212-savepoint-assembly-0.1.0-SNAPSHOT.jar

Main class for the a Flink job: __com.github.novakovalexey.Main__

# Switch Flink version

In order to switch to Flink 1.15.x or later version, you need to remove second % (percent) sign in the SBT `libraryDependencies` for most of the Flink dependencies. For example:

```scala
"org.apache.flink" %% "flink-streaming-java"
```
change to 

```scala
"org.apache.flink" % "flink-streaming-java"
```

this will make SBT to not add a Scala version suffix like _2.12 to the end of artifactId when
downloading from the Maven central.