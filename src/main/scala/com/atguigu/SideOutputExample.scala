package com.atguigu

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
//如果温度低于60摄氏度则侧输出
object SideOutputExample {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val readings = env.addSource(new SensorSource).keyBy(_.id)
    val monitoredReadings = readings.process(new FreezingMonitor)
    monitoredReadings.getSideOutput(new OutputTag[String]("freezing-alarms")).print()
    env.execute("sideoutput")

  }

}

class FreezingMonitor extends KeyedProcessFunction[String,SensorReading,SensorReading]{
  //定义一个侧输出标签
  lazy val freezingAlarmOutput = new OutputTag[String]("freezing-alarms")

  override def processElement(i: SensorReading,
                              context: KeyedProcessFunction[String, SensorReading, SensorReading]#Context,
                              collector: Collector[SensorReading]): Unit = {
    if(i.temperature <= 60){
      //将数据发送到侧输出流中
      context.output(freezingAlarmOutput,s"Freaazing alarm for ${i.id}")
    }
    //其他正常的数据则正常的输出到下游
    collector.collect(i)

  }
}
