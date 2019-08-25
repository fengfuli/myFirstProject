package streaming.transformation

import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

object BatchDistinct {

  def main(args: Array[String]): Unit = {

    val env=ExecutionEnvironment.getExecutionEnvironment
    val data=ListBuffer[String]()

    data.append("hello you")
    data.append("hello me")
    val text=env.fromCollection(data)


    val  mapData=text.flatMap(
      line=> {
        val words=line.split("\\W+")
        for (word <- words){
          println("原单词:"+word+"\n")
        }
        words
      }
    )

    mapData.distinct().print()






  }

}
