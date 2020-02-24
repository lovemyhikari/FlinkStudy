package com.atguigu

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object LateDataExample {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val source: DataStream[String] = env.socketTextStream("hadoop103",9999,'\n')
    val mainStream = source.map(data => {
      val splits: Array[String] = data.split(" ")
      (splits(0), splits(1).toLong * 1000)
    }).assignAscendingTimestamps(r => r._2)
      .keyBy(_._1)
      .process(new MyFunction)
    mainStream.getSideOutput(new OutputTag[(String,Long)]("late data")).print()
    env.execute("latedataTest")
  }

  class MyFunction extends KeyedProcessFunction[String,(String,Long),(String,Long)]{

    val side = new OutputTag[(String,Long)]("late data")

    override def processElement(i: (String, Long),
                                context: KeyedProcessFunction[String, (String, Long), (String, Long)]#Context,
                                collector: Collector[(String, Long)]): Unit = {
      if(i._2 < context.timerService().currentWatermark()){
        context.output(side,i)
      }else {
        collector.collect(i)
      }
    }
  }

}
