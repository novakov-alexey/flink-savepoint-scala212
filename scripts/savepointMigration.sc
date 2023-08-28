
//> using jvm adopt:11
//> using scala 2.12
//> using dep "org.apache.flink::flink-streaming-scala:1.14.4"
//> using dep "org.apache.flink::flink-clients:1.14.4"

import org.apache.flink.streaming.api.scala._

val env = StreamExecutionEnvironment.getExecutionEnvironment

val text = env.fromElements(
  "To be, or not to be,--that is the question:--",
  "Whether 'tis nobler in the mind to suffer",
  "The slings and arrows of outrageous fortune",
  "Or to take arms against a sea of troubles,"
)

text
  .flatMap(_.toLowerCase.split("\\W+"))
  .map((_, 1))
  .keyBy(_._1)
  .sum(1)
  .print()

env.execute("wordCount")
