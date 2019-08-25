package streaming

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaProducer
//import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper

object StreamingKafkaSink {


  def main(args: Array[String]): Unit = {


    val env=StreamExecutionEnvironment.getExecutionEnvironment


    val text=env.socketTextStream("192.168.137.128",9000,'\n')


    val topic="order_sql"
    val prop=new Properties()

    prop.setProperty("bootstrap.servers","192.168.137.128:9092")

    //val myPro=new FlinkKafkaProducer011[String](topic,new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),prop,FlinkKafkaProducer011.Semantic.EXACTLY_ONCE)

   // val myPro1=new FlinkKafkaProducer011[String]()
//new FlinkKafkaProducer011[String](prop,topic,new SimpleStringSchema);
  //  text.addSink(myPro)   // socket 数据写到了 生产者， 然后消费者可以进行消费了  说明写入成功



    val myProducer = new FlinkKafkaProducer011[String](
      "192.168.137.128:9092",         // broker list
      topic,               // target topic
      new SimpleStringSchema)   // serialization schema
    text.addSink(myProducer);
    env.execute("kafka sink")



  }

}
