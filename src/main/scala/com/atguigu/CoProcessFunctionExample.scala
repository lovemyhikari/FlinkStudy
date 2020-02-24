package com.atguigu

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.api.scala.typeutils.Types

//需求：设置一个控制流关闭的开关
object CoProcessFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 传感器读数
    val readings = env.addSource(new SensorSource)

    // 开关流
    val filterSwitches = env.fromCollection(Seq(
      ("sensor_2", 10 * 1000L) // id 为 2 的传感器读数放行 10s
    ))

    val forwardedReadings = readings.keyBy(_.id).connect(filterSwitches.keyBy(_._1)).process(new ReadingFilter)

    forwardedReadings.print()

    env.execute()
  }

}

class ReadingFilter extends CoProcessFunction[SensorReading,(String,Long),SensorReading]{

  // 没有的话，就初始化一个
  lazy val forwardingEnabled : ValueState[Boolean] = getRuntimeContext.getState(
    new ValueStateDescriptor[Boolean]("filterSwitch", Types.of[Boolean])
  )

  // 保存关闭开关的定时器的时间戳
  lazy val disableTimer : ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("timer", Types.of[Long])
  )

  override def processElement1(in1: SensorReading,
                               context: CoProcessFunction[SensorReading, (String, Long),
                                 SensorReading]#Context,
                               collector: Collector[SensorReading]): Unit = {
    if(forwardingEnabled.value){
      collector.collect(in1)
    }

  }

  override def processElement2(in2: (String, Long),
                               context: CoProcessFunction[SensorReading, (String, Long),
                                 SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    forwardingEnabled.update(true)
    val time: Long = context.timerService().currentProcessingTime() + in2._2
    context.timerService().registerProcessingTimeTimer(time)
    disableTimer.update(time)

  }

  override def onTimer(timestamp: Long,
                       ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext,
                       out: Collector[SensorReading]): Unit = {
    forwardingEnabled.clear()
    disableTimer.clear()
  }
}
