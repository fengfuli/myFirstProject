package streaming

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011




object StreamingKafkaSource {


  def main(args: Array[String]): Unit = {


    val env=StreamExecutionEnvironment.getExecutionEnvironment


    val topic="order_sql"
    val prop=new Properties()

    prop.setProperty("bootstrap.servers","192.168.137.128:9092")
    prop.setProperty("group.id","test-consumer-group")

    val myConsumer=new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),prop)
    val text=env.addSource(myConsumer)
    text.print()

    env.execute("kafka scala")



  }

}
