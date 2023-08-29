package com.github.novakovalexey

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
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
import WordCounter._
import org.apache.flink.api.java.functions.KeySelector

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

object WordCounter {
  val stateDescriptor =
    new MapStateDescriptor("wordCounter", classOf[Int], classOf[WordCountState])
}

class WordCounter extends KeyedProcessFunction[Int, Event, Event] {
  // MapState is used here to check serialization. ValueState would be more efficient
  @transient var countState: MapState[Int, WordCountState] = _

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
        System.currentTimeMillis
      )
    )
    collector.collect(event.copy(count = count))
  }
}

object Main extends App {
  val conf = new Configuration()
  //conf.setString("state.savepoints.dir", "file:///tmp/savepoints")
  // val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env
    .addSource(FakeSource.iterator)
    .keyBy(new KeySelector[Event, Int] {
      override def getKey(value: Event): Int = value.number
    })
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
  ): Unit =
    out.collect(
      WordCountState(key, countState.get(key).count, System.currentTimeMillis)
    )
}

object ReadState extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment
  val savepoint = Savepoint.load(
    env,
    "/tmp/flink-savepoints/savepoint-5103b1-065fc210b104",
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
