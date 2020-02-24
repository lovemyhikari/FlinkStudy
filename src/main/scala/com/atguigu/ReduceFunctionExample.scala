package com.atguigu

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
// 需求：计算每个传感器 15s 窗口中的温度最小值
object ReduceFunctionExample {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val source: DataStream[SensorReading] = env.addSource(new SensorSource)
    val sensorStream: DataStream[(String, Double)] = source.map(data => (data.id,data.temperature))
    val result: DataStream[(String, Double)] = sensorStream
      .keyBy(0).timeWindow(Time.seconds(15)).reduce((x,y) => (x._1,x._2.min(y._2)))
    result.print()
    env.execute("reduce function")

  }

}
