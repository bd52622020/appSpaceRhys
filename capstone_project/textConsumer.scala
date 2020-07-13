package radio

import java.io.NotSerializableException
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecord}
import org.apache.spark.sql.{SparkSession,SaveMode}
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
import java.util.Calendar
import java.text.SimpleDateFormat

object textConsumer {
 
  //class for json extraction
  case class kMessage(timestamp: Int, transcript: String)  
  
  def main(args: Array[String]){
    streamFromKafka(Array("transcripts",
        "/home/data/project/logs/kafka_success/kafka_consumers",
        "local[*]",
        "textConsume",
        "mongodb://127.0.0.1:27017/radio.transcripts",
	      "resolve_canonical_bootstrap_servers_only",
	      "localhost:9092",
	      "SparkConsumer",
	      "earliest"))
  }
  
  def streamFromKafka(args: Array[String]) {
     
      //Initialise Spark Streaming
      val sConf:SparkConf = getSparkConf(args(2),args(3),args(4))                                
      val ssc:StreamingContext = new StreamingContext(sConf, Seconds(3))                               
      ssc.sparkContext.setLogLevel("ERROR")
      
      //Kafka Consumer Configuration
      val kafkaConfig:Map[String, Object] = getKafkaConf(args(5),args(6),args(7),args(8))
      
      //Initialise Kafka Stream
      val kafkaRawStream:InputDStream[ConsumerRecord[String, String]] = kafkaStreamInit(ssc,args(0),kafkaConfig)
      
      //mongo writer configuration
      val mongoConfig:WriteConfig = getMongoConfig(ssc,args(4))
            
      try{
        messageReceiptLog(kafkaRawStream,args(1))                                   
      }
      catch {
        case e:Exception=> println(e)
      }      
      try{
        val cStream:DStream[Document] = computeStream(kafkaRawStream) 
        publishToMongo(cStream,mongoConfig)                                   
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
    
    //compute records and insert into mongo
    def computeStream(kafkaRawStream:InputDStream[ConsumerRecord[String, String]]):DStream[Document] ={
      kafkaRawStream.map(record => (record.key:String,jsonParse(record.value)))
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
    }
    
    def publishToMongo(kafkaStream:DStream[Document],mongoConfig:WriteConfig):Unit = {
      kafkaStream.foreachRDD(rdd=>MongoSpark.save(rdd,mongoConfig)) //export to mongo 
    }
    
    //Log message receipt
    def messageReceiptLog(kafkaRawStream:InputDStream[ConsumerRecord[String, String]],SuccessLogs:String):Unit ={
      //date formatter      
      val dateTimeFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") 
      kafkaRawStream.map(record => {val now = dateTimeFormat.format(Calendar.getInstance().getTime());
                                    (s"${record.topic} ${record.partition} ${record.offset},${now},INFO")})
                    .foreachRDD(rdd =>{ 
                        if (rdd.take(1).length == 1){
                          val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
                          import spark.implicits._      
                          rdd.toDF.coalesce(1).write.format("text").mode(SaveMode.Append).save(SuccessLogs)}})      
    }
    
    //get spark configuration
    def getSparkConf(master:String,appName:String,mongoURI:String):SparkConf = {
       new SparkConf().setMaster(master).setAppName(appName).set("spark.mongodb.output.uri", mongoURI)     
    }
    
    //get kafka configuration
    def getKafkaConf(dns:String,bootstrap:String,id:String,reset:String):Map[String, Object] = {
       Map[String, Object](
                "client.dns.lookup" -> dns,
                "bootstrap.servers" -> bootstrap,
                "key.deserializer" -> classOf[StringDeserializer],
                "value.deserializer" -> classOf[StringDeserializer],
                "group.id" -> id,
                "auto.offset.reset" -> reset,
                "enable.auto.commit" -> (true: java.lang.Boolean))     
    }
    
    def kafkaStreamInit(ssc:StreamingContext,topic:String,kafkaConfig:Map[String, Object]):InputDStream[ConsumerRecord[String, String]] = {
       createDirectStream[String, String](
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaConfig))     
    }
    
    def getMongoConfig(ssc:StreamingContext,mongoURI:String):WriteConfig = {
      WriteConfig(Map("spark.mongodb.output.uri" -> mongoURI), Some(WriteConfig(ssc.sparkContext)))
    }
}