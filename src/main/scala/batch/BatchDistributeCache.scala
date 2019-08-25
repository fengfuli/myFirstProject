package batch

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object BatchDistributeCache {


  def main(args: Array[String]): Unit = {

    //类似于Hadoop的分布式缓存  让并行运行的实例的函数可以在本地访问。这个功能可以被使用来分享外部静态的数据。
    // flink 会自动将文件复制到所有worker节点的本地文件系统   函数可以根据名字取该节点的本地文件系统中检索该文件。

    val  env=ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    env.registerCachedFile("E:\\ffl\\data\\aaa.txt","a.txt")
    val  data= env.fromElements("a","b","c","d")

    val res=data.map(new RichMapFunction[String,String] {

      override def open(parameters: Configuration): Unit = {super.open(parameters)

          val myFile=getRuntimeContext.getDistributedCache.getFile("a.txt")

         val lines =FileUtils.readLines(myFile)

        val it =  lines.iterator()
        while(it.hasNext){
          val  next = it.next()
          print("line :"+ next +"\n")
        }

      }

      override def map(in: String): String = {
              in
      }

    })

      res.print()



  }

}
