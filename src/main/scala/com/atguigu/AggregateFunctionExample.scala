package com.atguigu

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
object AggregateFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sensorData = env.addSource(new SensorSource)
    val avgTempPerWindow = sensorData
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      // 注意下面是 aggregate
      .aggregate(new AvgTempFunction)

    avgTempPerWindow.print()

    env.execute()
  }

}

class AvgTempFunction extends AggregateFunction[(String,Double),(String,Double,Int),(String,Double)]{
  //对累加器进行初始化
  override def createAccumulator(): (String, Double, Int) = ("",0.0,0)

  override def add(in: (String, Double), acc: (String, Double, Int)): (String, Double, Int) = {
    (in._1,acc._2+in._2,acc._3+1)
  }

  override def getResult(acc: (String, Double, Int)): (String, Double) = (acc._1,acc._2 / acc._3)

  override def merge(acc: (String, Double, Int), acc1: (String, Double, Int)): (String, Double, Int) = {
    (acc._1,acc._2+acc1._2,acc._3+acc1._3)
  }
}