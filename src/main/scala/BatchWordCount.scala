import org.apache.flink.api.scala.ExecutionEnvironment

object BatchWordCount {


  def main(args: Array[String]): Unit = {

    val inputPath = "E:\\ffl\\data"
    val output="E:\\ffl\\data\\data"

    val env=ExecutionEnvironment.getExecutionEnvironment
    val text=env.readTextFile(inputPath)
   //隐式转换
    import org.apache.flink.api.scala._
    val counts=text.flatMap(_.toLowerCase().split("\\W+"))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    counts.writeAsCsv(output).setParallelism(1)
    env.execute("batch word count")

  }
}
