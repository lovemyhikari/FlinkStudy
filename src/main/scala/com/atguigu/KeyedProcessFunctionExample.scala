package com.atguigu

import com.atguigu.SensorSource
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedProcessFunctionExample {

  // KeyedProcessFunction只能操作KeyedStream
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val warnings: DataStream[String] = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      // 注意是 process ！
      .process(new TempIncreaseAlertFunction)

    warnings.print()

    env.execute()
  }

}

class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {

  //保存上一次传感器温度的状态变量
  lazy val lastTemp = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("lastTemp",Types.of[Double])
  )

  //保存定时器时间戳的状态变量,该时间戳可以出发onTimer函数
  lazy val currentTimer : ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("timer", Types.of[Long])
  )

  //每次来一个元素都会调用
  override def processElement(i: SensorReading,
                              context: KeyedProcessFunction[String, SensorReading, String]#Context,
                              collector: Collector[String]): Unit = {
    //对上一次的温度进行更新
    val preTemp = lastTemp.value
    //对状态变量进行更新
    lastTemp.update(i.temperature)
    val curTimerTimestamp = currentTimer.value

    if(preTemp == 0.0 || i.temperature < preTemp){
      context.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
      currentTimer.clear()
    }else if (i.temperature > preTemp && curTimerTimestamp == 0){
      val timerTs: Long = context.timerService().currentProcessingTime() + 1000
      context.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(timerTs)
    }
  }

  // onTimer 向下发送数据
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    out.collect("传感器id为： " + ctx.getCurrentKey + "的传感器温度值已经连续 1s 上升了。")
    // 别忘了清空状态变量
    currentTimer.clear()
  }

}
