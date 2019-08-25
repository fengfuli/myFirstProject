package streaming.transformation

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

object BatchFirst {

  def main(args: Array[String]): Unit = {

    val env=ExecutionEnvironment.getExecutionEnvironment


    val data=ListBuffer[Tuple2[Int,Int]]()

    data.append((1,7))
    data.append((5,5))
    data.append((2,15))
    data.append((3,45))
    data.append((1,5))
    data.append((3,5))
    data.append((2,6))


  val text=env.fromCollection(data)
    println("---------前三个-----")
    text.first(3).print()
    println("---------第一列分组获取每组前两个元素-----")
    text.groupBy(0).first(2).print()

    println("---------sort-----")
    text.sortPartition(0,Order.ASCENDING).sortPartition(1,Order.ASCENDING).first(4).print()




  }

}
