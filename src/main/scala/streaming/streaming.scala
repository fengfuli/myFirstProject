package streaming

import org.apache.flink.streaming.api.scala._


object streaming {

  def main(args: Array[String]): Unit = {

    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val data=List(15,20,17)


    val text = env.fromCollection(data)

    val num=text.map(_+1)

    num.print().setParallelism(1)
    env.execute("streaming")

  }

}
