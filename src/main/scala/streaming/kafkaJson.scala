package scala.streaming

import org.apache.flink.api.common.state.StateDescriptor.Type
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.apache.flink.types.Row

object kafkaJson {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // create a TableEnvironment for streaming queries
    val tableEnv = StreamTableEnvironment.create(env);

    tableEnv
      .connect(
        new Kafka()
          .version("0.11")
          .topic("order")
          .startFromEarliest()
          .property("zookeeper.connect", "localhost:2181")
          .property("bootstrap.servers", "localhost:9092")

      )
      .withFormat(
        new Json().failOnMissingField(false)
      .deriveSchema()
      )
      .withSchema(
        new Schema()
          .field("order_id",Types.STRING)
          .field("shop_id",Types.STRING)
          .field("member_id",Types.STRING)
          .field("trade_amt",Types.STRING)
          .field("pay_time",Types.STRING)
      )
      .inAppendMode()
      .registerTableSource("sm_user")
   val  result = tableEnv.sqlQuery("select * from sm_user")
    tableEnv.toAppendStream[Row](result).print().setParallelism(1)
    env.execute("example")
  }


}
