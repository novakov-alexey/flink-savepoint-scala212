package com.example

import org.apache.flink.api.common.state.{
  ReducingStateDescriptor,
  ValueState,
  ValueStateDescriptor
}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.state.api.Savepoint
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.state.api.functions.KeyedStateReaderFunction.Context
import org.apache.flink.util.Collector

import scala.util.Random
import scala.collection.JavaConverters._

case class Event(number: Int, count: Int = 1)

object FakeSource {
  val iterator = new FromIteratorFunction[Event](
    (new Iterator[Event] with Serializable {
      val rand = new Random()

      override def hasNext: Boolean = true

      override def next(): Event = {
        Thread.sleep(1000)
        Event(rand.nextInt(20))
      }
    }).asJava
  )
}

class WordCounter extends KeyedProcessFunction[Int, Event, Event] {
  @transient var countState: ValueState[Int] = _

  override def open(parameters: Configuration): Unit =
    countState = getRuntimeContext.getState(
      new ValueStateDescriptor("wordCounter", classOf[Int])
    )

  @throws[Exception]
  def processElement(
      event: Event,
      context: KeyedProcessFunction[Int, Event, Event]#Context,
      collector: Collector[Event]
  ): Unit = {
    countState.update(
      event.count + countState.value
    )
    collector.collect(event.copy(count = countState.value))
  }
}

object Main extends App {
  val conf = new Configuration()
  conf.setString("state.savepoints.dir", "file:///tmp/savepoints")

  val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
  env
    .addSource(FakeSource.iterator)
    .keyBy(e => e.number)
    .process(new WordCounter())
    .uid("word-count")
    .print()

  env.execute("Scala WordCount Example")
}

case class WordCountState(key: Int, count: Int)

class ReaderFunction extends KeyedStateReaderFunction[Int, WordCountState] {
  var countState: ValueState[Int] = _

  override def open(parameters: Configuration): Unit =
    countState = getRuntimeContext
      .getState(
        new ValueStateDescriptor("wordCounter", classOf[Int])
      )

  override def readKey(
      key: Int,
      ctx: Context,
      out: Collector[WordCountState]
  ): Unit =
    out.collect(WordCountState(key, countState.value))
}

object ReadState extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment
  val savepoint = Savepoint.load(
    env,
    "/tmp/flink-savepoints/savepoint-03414b-f0527590c05a",
    new HashMapStateBackend()
  )
  val keyedState = savepoint.readKeyedState(
    "word-count",
    new ReaderFunction(),
    TypeInformation.of(classOf[Int]),
    TypeInformation.of(classOf[WordCountState])
  )
  val res = keyedState.collect().asScala
  println(res.mkString("\n"))
}
