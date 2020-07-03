package radio

import java.io.NotSerializableException
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecord}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.streaming.{StreamingContext, Seconds, Minutes}
import org.apache.spark.streaming.dstream.{InputDStream,DStream}
import org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream
import org.apache.spark.streaming.kafka010.{LocationStrategies,ConsumerStrategies}
import org.apache.kafka.common.serialization.StringDeserializer
import com.mongodb.spark.config._
import com.mongodb.spark.MongoSpark
import org.bson.Document
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization


object textConsumer {
  def main(args: Array[String]){
    streamFromKafka(Array("transcripts"))
  }
   def streamFromKafka(kafkaTopics:Array[String]) {
     
      //spark Configuration
      val sparkConf = new SparkConf().setMaster("local[*]")
                                     .setAppName("textConsume")
                                     .set("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/radio.transcripts")
      
      //Initialise Spark
      val ssc:StreamingContext = new StreamingContext(sparkConf, Minutes(1))                               
      ssc.sparkContext.setLogLevel("ERROR")
      
      //Kafka Consumer Configuration
      val kafkaConfig = Map[String, Object](
          "client.dns.lookup" -> "resolve_canonical_bootstrap_servers_only",
          "bootstrap.servers" -> "localhost:9092",
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "SparkConsumer",
          "auto.offset.reset" -> "latest",
          "enable.auto.commit" -> (true: java.lang.Boolean)
      )
      
      //Initialise Kafka Stream
      val kafkaRawStream: InputDStream[ConsumerRecord[String, String]] =      
        createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](kafkaTopics, kafkaConfig)
        )
      try{
        //
        case class kMessage(timestamp: Int, transcript: String)
        val mongoConfig = WriteConfig(Map("spark.mongodb.output.uri" -> "mongodb://127.0.0.1:27017/radio.transcipts"), Some(WriteConfig(ssc.sparkContext)))
        val textStream = kafkaRawStream.map(record => (record.key:String,jsonParse(record.value)))
                                        .groupByKeyAndWindow(Minutes(1)) //collate windows of messages 
                                        .map(row=>( 
                                            row._1, //radio station
                                            row._2.map(x=>x._1.toLong), //timestamps
                                            row._2.map(x=>x._2).mkString(" "), //raw text
                                            row._2.map(x=>x._2.split(" ")).flatten.groupBy(identity).mapValues(_.size))) //create map of words -> frequency
                                        .map(row => {implicit val formats = DefaultFormats;
                                                      "{'station':'"+ row._1 + "'," +
                                                      "'timestamps':" + Serialization.write(row._2).toString() + "," +
                                                      "'raw_text':'" + row._3 + "'," +
                                                      "'tokenized_text':" + Serialization.write(row._4).toString() + "}"})
                                        .map(row=> Document.parse(row)) //convert json string to bson
                                        .foreachRDD(rdd=>MongoSpark.save(rdd,mongoConfig)) //export to mongo                                       
      }
      catch {
        case e:Exception=> println(e)
      }
      
      ssc.start() // start the computation
      ssc.awaitTermination()
       
        
   }
   //Parse JSON into tuple
    def jsonParse(jstring: JsonInput): Tuple2[String,String] = {
      case class kMessage(timestamp: String, transcript: String)
      implicit val formats = DefaultFormats;
      val km = parse(jstring).extract[kMessage]
      (km.timestamp.toString,km.transcript.toString.replaceAll("['.]",""))
    }   
}