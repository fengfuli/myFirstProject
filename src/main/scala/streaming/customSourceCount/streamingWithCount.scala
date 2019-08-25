package streaming.customSourceCount

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object streamingWithCount {
  def main(args: Array[String]): Unit = {

    val env=StreamExecutionEnvironment.getExecutionEnvironment
    // val data=List(15,20,17)


    var text=env.addSource(new customSource)

    val mapData=text.map(line=>{
      println("接收到的数据："+line)
      line
    })

    val sum=mapData.timeWindowAll(Time.seconds(2)).sum(0)
    sum.print().setParallelism(1)
    env.execute("streaming")

  }
}
