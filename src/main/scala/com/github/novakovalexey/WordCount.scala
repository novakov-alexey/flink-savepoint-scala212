package com.github.novakovalexey

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.operators.DataSource
import org.apache.flink.runtime.state.{
  FunctionInitializationContext,
  FunctionSnapshotContext
}
import org.apache.flink.state.api.{
  BootstrapTransformation,
  SavepointReader,
  SavepointWriter
}
import org.apache.flink.state.api.functions.{
  KeyedStateBootstrapFunction,
  StateBootstrapFunction
}
import org.apache.flink.api.scala.ExecutionEnvironment
// import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.state.api.Savepoint
import org.apache.flink.state.api.OperatorTransformation
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.state.api.functions.KeyedStateReaderFunction.Context
import org.apache.flink.util.Collector
import WordCounter._

import scala.util.Random
import scala.collection.JavaConverters._

object FakeSource {
  val iterator = new FromIteratorFunction[Event](
    new Iterator[Event] with Serializable {
      val rand = new Random()

      override def hasNext: Boolean = true

      override def next(): Event = {
        Thread.sleep(1000)
        Event(rand.nextInt(20))
      }
    }.asJava
  )
}

object WordCounter {
  val stateDescriptor =
    new MapStateDescriptor(
      "wordCounter",
      TypeInformation.of(classOf[Int]),
      // createTypeInformation[Int],
      TypeInformation.of(classOf[WordCountState])
      // createTypeInformation[WordCountState]
    )
}

class WordCounter extends KeyedProcessFunction[Int, Event, Event] {
  // MapState is used here to check serialization. ValueState would be enough to count numbers
  private var countState: MapState[Int, WordCountState] = _

  override def open(parameters: Configuration): Unit =
    countState = getRuntimeContext.getMapState(stateDescriptor)

  @throws[Exception]
  def processElement(
      event: Event,
      context: KeyedProcessFunction[Int, Event, Event]#Context,
      collector: Collector[Event]
  ): Unit = {
    val count =
      if (countState.contains(event.number)) countState.get(event.number).count
      else 0
    countState.put(
      event.number,
      WordCountState(
        event.number,
        event.count + count,
        System.currentTimeMillis,
        Some(count)
      )
    )
    collector.collect(event.copy(count = count))
  }
}

object Main extends App {
  val conf = new Configuration()
  conf.setString("state.savepoints.dir", "file:///tmp/savepoints")
  val env = StreamExecutionEnvironment.getExecutionEnvironment(conf)
  // env.getConfig.disableGenericTypes()

  val eventTi = TypeInformation.of(classOf[Event])

  env
    .addSource(FakeSource.iterator)
    .returns(eventTi)
    .keyBy((e: Event) => e.number, TypeInformation.of(classOf[Int]))
    // .keyBy(_.number, createTypeInformation[Int])
    .process(new WordCounter())
    .uid("word-count")
    .print()

  env.execute("Scala WordCount Example")
}

class ReaderFunction extends KeyedStateReaderFunction[Int, WordCountState] {
  var countState: MapState[Int, WordCountState] = _

  override def open(parameters: Configuration): Unit =
    countState = getRuntimeContext.getMapState(stateDescriptor)

  override def readKey(
      key: Int,
      ctx: Context,
      out: Collector[WordCountState]
  ): Unit = {
    val state = countState.get(key)
    out.collect(
      WordCountState(
        key,
        state.count,
        System.currentTimeMillis,
        state.lastCount
      )
    )
  }
}

object ReadState extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val oldSavepointPath = "/tmp/flink-savepoints/savepoint-7fb950-384cc7627885"
  val savepoint = SavepointReader.read(
    env,
    oldSavepointPath,
    new HashMapStateBackend()
  )

  val keyedState = savepoint.readKeyedState(
    "word-count",
    new ReaderFunction(),
    TypeInformation.of(classOf[Int]),
    // createTypeInformation[Int], // comes from flink-scala-api
    TypeInformation.of(classOf[WordCountState])
    // createTypeInformation[WordCountState] // comes from flink-scala-api
  )
  val res = keyedState.executeAndCollect().asScala
  println(res.mkString("\n"))

  val transformation = OperatorTransformation
    .bootstrapWith(keyedState)
    .keyBy(
      (value: WordCountState) => value.key,
      TypeInformation.of(classOf[Int])
    )
    .transform(new KeyedStateBootstrapFunction[Int, WordCountState] {
      private var countState: MapState[Int, WordCountState] = _

      override def open(parameters: Configuration): Unit = {
        val descriptor = new MapStateDescriptor(
          "wordCounter",
          TypeInformation.of(classOf[Int]),
          TypeInformation.of(classOf[WordCountState])
        ) // this target state descriptor, which can be used to use different serializers / type info
        countState = getRuntimeContext.getMapState(descriptor)
      }

      override def processElement(
          value: WordCountState,
          ctx: KeyedStateBootstrapFunction[Int, WordCountState]#Context
      ): Unit =
        countState.put(value.key, value)
    })

  SavepointWriter
    .fromExistingSavepoint(oldSavepointPath)
    .removeOperator("word-count")
    .withOperator("word-count", transformation)
    .write(oldSavepointPath.replaceAll("savepoint-", "new-savepoint-"))

  env.execute()
}
