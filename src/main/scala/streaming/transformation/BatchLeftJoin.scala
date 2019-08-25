package streaming.transformation

import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

object BatchLeftJoin {
  def main(args: Array[String]): Unit = {

    val env=ExecutionEnvironment.getExecutionEnvironment


    val data=ListBuffer[Tuple2[Int,String]]()
    data.append((1,"2s"))
    data.append((2,"2s"))
    data.append((3,"2s"))


    val data2=ListBuffer[Tuple2[Int,String]]()
    data2.append((1,"beijing"))
    data2.append((2,"guangdong"))
    data2.append((4,"shanghai"))


    val text1=env.fromCollection(data)
    val text2=env.fromCollection(data2)

    text1.leftOuterJoin(text2).where(0).equalTo(0).apply((first,second)
    =>{
      if (second==null){
        (first._1,first._2,"null")
      }else{
        (first._1,first._2,second._2)
      }
    }
    ).print()








  }

}
