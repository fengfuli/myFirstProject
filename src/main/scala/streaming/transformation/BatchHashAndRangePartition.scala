package streaming.transformation

import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

object BatchHashAndRangePartition {

  def main(args: Array[String]): Unit = {

    val env=ExecutionEnvironment.getExecutionEnvironment


    val data=ListBuffer[Tuple2[Int,String]]()
    data.append((1,"hello1"))
    data.append((2,"hello2"))
    data.append((2,"hello3"))
    data.append((2,"hello4"))
    data.append((2,"hello5"))
    data.append((2,"hello6"))
    data.append((2,"hello7"))
    data.append((2,"hello8"))
    data.append((2,"hello9"))
    data.append((2,"hello10"))
    data.append((2,"hello11"))
    data.append((2,"hello12"))
    data.append((2,"hello13"))
    data.append((2,"hello14"))
    data.append((3,"hello15"))
    data.append((3,"hello16"))
    data.append((3,"hello17"))
    data.append((3,"hello18"))

    val text=env.fromCollection(data)

    text.partitionByHash(0).mapPartition(
      it =>{
        while (it.hasNext){
          val tu = it.next()
          println("当前线程id: " +Thread.currentThread.getId+","+tu)

        }
        it
      }
    ).print()

    println("--------------range----------------------")
    text.partitionByRange(0).mapPartition(
      it =>{
        while (it.hasNext){
          val tu = it.next()
          println("当前线程id: " +Thread.currentThread.getId+","+tu)

        }
        it
      }
    ).print()








  }

}
