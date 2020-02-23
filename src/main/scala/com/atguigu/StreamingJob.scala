package com.atguigu

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingJob {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text: DataStream[String] = env.socketTextStream("hadoop103",9999,'\n')
    val windowCounts: DataStream[WordWithCount] = text.flatMap(_.split("\\s")).map(word => WordWithCount(word, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5)).sum("count")
    windowCounts.print().setParallelism(1)
    env.execute("Socket Window WordCount")

  }

  case class WordWithCount(word: String, count: Long)

}
