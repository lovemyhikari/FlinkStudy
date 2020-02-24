package com.atguigu

import com.atguigu.ProcessWindowFunctionExample.MinMaxTemp
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
// 需求：计算窗口中最大温度和最小温度，附加上窗口结束时间
object ProcessWindowFunctionExample {

  case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sensorData = env.addSource(new SensorSource)

    val minMaxTempPerWindow = sensorData
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .process(new HighAndLowTempProcessFunction)

    minMaxTempPerWindow.print()

    env.execute()
  }

}

class HighAndLowTempProcessFunction extends ProcessWindowFunction[SensorReading,MinMaxTemp,String,TimeWindow]{
  //当窗口关闭的时候会调用这个函数
  override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[MinMaxTemp]): Unit = {
    val temp: Iterable[Double] = elements.map(_.temperature)
    val end: Long = context.window.getEnd
    out.collect(MinMaxTemp(key,temp.max,temp.min,end))

  }
}