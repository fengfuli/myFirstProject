import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object SocketWindowWordCount {
  def main(args: Array[String]): Unit = {

    val port: Int =try {
      ParameterTool.fromArgs(args).getInt("port")
    }catch {
      case e:Exception=>{
        System.err.println("NO PORT SET USER DEFAULT PORT 9000")
      }
        9000
    }

   val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val text= env.socketTextStream("192.168.137.128",port)

    // 解析数据（把数据打平） 分组 窗口计算 并且聚合
   import org.apache.flink.api.scala._
    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(2),Time.seconds(1))
      .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }

  /** Data type for words with count */
  case class WordWithCount(word: String, count: Long)

}
