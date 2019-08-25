package batch

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

object BatchBroadCast {

  def main(args: Array[String]): Unit = {

    val env=ExecutionEnvironment.getExecutionEnvironment
    val data=ListBuffer[Tuple2[String,Int]]()
    data.append(("2z",18))
    data.append(("1s",19))
    data.append(("3s",17))

    val tupleData=env.fromCollection(data)

    val brodCastData=tupleData.map(
      tup=>{
        Map(tup._1->tup._2)
      }
    )


    val text= env.fromElements("2z","1s","3s")

      // 接受类型是 STRING 返回类型是  STRING
   var result  =  text.map(new RichMapFunction[String,String] {



      var listData : java.util.List[Map[String,Int]] = null

      var allMap = Map[String,Int]()

      override def open(parameters: Configuration): Unit = {

        super.open(parameters)

        this.listData= getRuntimeContext.getBroadcastVariable[Map[String,Int]]("broadCastMapName")

        val it = listData.iterator()

        while (it.hasNext){
          val next = it.next()
             allMap=allMap.++(next)

        }

      }
         //输入 STRING 返回 STRING
      override def map(in: String): String = {
         val age  =   allMap.get(in).get

        in + " " + age
      }
    }).withBroadcastSet(brodCastData,"broadCastMapName")




result.print()






  }

}
