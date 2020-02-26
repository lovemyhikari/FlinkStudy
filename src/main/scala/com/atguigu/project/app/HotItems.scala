package com.atguigu.project.app

import java.sql.Timestamp

import com.atguigu.project.bean.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItems {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source: DataStream[String] = env.readTextFile("D:\\DevSoftware\\IntelliJ IDEA 2019.2.2\\workspace\\Flink0830\\src\\main\\resources\\UserBehavior.csv")
    val stream: DataStream[String] = source.map(data => {
      val splits: Array[String] = data.split(",")
      //将数据封装到样例类中 -- 时间戳必须转换成毫秒级的
      UserBehavior(splits(0).toLong, splits(1).toLong, splits(2).toInt, splits(3), splits(4).toLong * 1000)
    }) //过滤出pv事件
      .filter(_.behavior == "pv")
      //设置watermark
      .assignAscendingTimestamps(_.timestamp)
      //先分组统计每个商品的浏览次数
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg, new WindowResultFunction)
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))
    stream.print
    env.execute("TopN")


  }

  //<K, I, O>
  class TopNHotItems(num:Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{
    //采用的是list状态变量，将每个元素的访问量全部缓存起来
    lazy val itemState = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("itemviewcount",Types.of[ItemViewCount])
    )
    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
      itemState.add(value)
      //注册定时事件
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val buffer: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for(item <- itemState.get){
        buffer += item
      }
      itemState.clear()
      //进行排序
      val sortedItems = buffer.sortBy(-_.count).take(num)
      val result = new StringBuilder
      result
        .append("==================================\n")
        .append("时间： ")
        .append(new Timestamp(timestamp - 1))
        .append("\n")
      for (i <- sortedItems.indices) {
        val currentItem = sortedItems(i)
        result
          .append("No")
          .append(i + 1)
          .append(": ")
          .append(" 商品ID = ")
          .append(currentItem.itemId)
          .append(" 浏览量 = ")
          .append(currentItem.count)
          .append("\n")
      }
      result
        .append("===================================")
      Thread.sleep(1000)
      out.collect(result.toString)

    }
  }

  //[IN, OUT, KEY, W <: Window] 当水位线超过窗口结束时间的时候触发
  class WindowResultFunction extends ProcessWindowFunction[Long,ItemViewCount,Long,TimeWindow]{
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key,context.window.getEnd,elements.iterator.next()))
    }

  }

  //创建一个累加器，用于统计每个id的浏览数
  class CountAgg extends AggregateFunction[UserBehavior,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

}
