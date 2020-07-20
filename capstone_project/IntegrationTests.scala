import org.scalatest.funsuite.AnyFunSuite
import radio.textConsumer._
import sys.process._
import scala.concurrent.{Future,Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.matching.Regex
import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Projections
import org.bson.Document
import scala.util.{Failure, Success}
import java.time._
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame,Row}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.Trigger
import better.files._



class IntegrationTests extends AnyFunSuite {
    
   test("pipeline should publish valid bson document to mongo from audio stream") {
     
     val pythonScriptsDir = "/home/data/eclipse-python/radio_project/src"
     val logsDir = "/home/data/eclipse_scala/radioProject/logs"
     
     val p1 = s"python3 $pythonScriptsDir/input.py teststation http://bbcmedia.ic.llnwd.net/stream/bbcmedia_radio5live_mf_p.m3u $logsDir".run()
     val p2 = s"python3 $pythonScriptsDir/transcript.py teststation test_transcripts $logsDir".run()
     
     Thread.sleep(40000)
     p1.destroy()
     Thread.sleep(20000)
     p2.destroy()
     
     try{
       Await.result(Future{streamFromKafka(Array("test_transcripts",
            "/home/data/eclipse_scala/radioProject/logs/kafka_success/kafka_producers",
            "local[*]",
            "textConsume",
            "mongodb://127.0.0.1:27017/test.transcripts",
    	      "resolve_canonical_bootstrap_servers_only",
    	      "localhost:9092",
    	      "SparkConsumer",
    	      "earliest"))}, 30 seconds)
     }
     catch{
       case ex: Exception=>println("streamFromKafka completed.")
     }
     
     val mongoClient = new MongoClient("localhost", 27017);
     val collection = mongoClient.getDatabase("test").getCollection("transcripts")
     val record = collection.find.first
     assert(record.get("station").toString() == "teststation")
     assert(record.get("raw_text").toString().split(" ").length > 10)
     assert("[0-9]{13}".r.findAllIn(record.get("timestamps").toString()).length > 0)
     assert("the=[1-9]".r.findAllIn(record.get("tokenized_text").toString()).length == 1)
     collection.drop()
     mongoClient.close()
   } 
   test("All producer message logs should have a corresponding consumer message log") {
     
       val logsDir:String = "/home/data/eclipse_scala/radioProject/kafka-test-logs"
       val SuccessLogs:String = f"$logsDir/kafka_success/*/*"
       val pythonScriptsDir:String = "/home/data/eclipse-python/radio_project/src"
       
       file"$logsDir/kafka_success/kafka_consumers".clear()
       file"$logsDir/kafka_success/kafka_producers".clear()
       
       val p1 = f"python3 $pythonScriptsDir/input.py teststation http://bbcmedia.ic.llnwd.net/stream/bbcmedia_radio5live_mf_p.m3u $logsDir".run()
       val p2 = f"python3 $pythonScriptsDir/transcript.py teststation test_transcripts $logsDir".run()
       
       Thread.sleep(40000)
       p1.destroy()
       Thread.sleep(40000)
       p2.destroy()
       
       try{
         Await.result(Future{streamFromKafka(Array("test_transcripts",
              f"$logsDir/kafka_success/kafka_producers",
              "local[*]",
              "textConsume",
              "mongodb://127.0.0.1:27017/test.transcripts",
      	      "resolve_canonical_bootstrap_servers_only",
      	      "localhost:9092",
      	      "SparkConsumer",
      	      "earliest"))}, 40 seconds)
       }
       catch{
         case ex: Exception=>println("streamFromKafka completed.")
       }      
    
      //Initialise Spark
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StructuredNetworkWordCount")  
      val spark:SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      import spark.sqlContext.implicits._
      
      val schema:StructType = StructType(Array(
          StructField("message", StringType, true),
          StructField("timestamp", TimestampType, true),
          StructField("Level", StringType, true)))
      
      //Watches log directory, loads all send/receive success logs and filters those that were not paired in each sliding window
      val logs = spark.readStream.option("sep", ",")
                                      .option("header", "false") 
                                      .schema(schema)
                                      .csv(f"$SuccessLogs/kafka_success/*/*")
                                      .groupBy(window(col("timestamp"),"180 seconds","60 seconds"),col("message"))
                                      .agg(count(col("message")).as("count"))
                                      .filter(col("count") === 1)
                                      .writeStream
                                      .queryName("failed")
                                      .outputMode("complete")
                                      .format("memory")
                                      .start()
                                                                                        
      logs.awaitTermination(60000)
      logs.stop()
      
      //displays messages that were not paired in three consecutive windows  
      val unpaired = spark.sql("select * from failed")
           .groupBy("message")
           .count
           .filter(col("count") === 3)
           .drop("count")
      assert(unpaired.count() == 0)     
      
      file"$logsDir/kafka_success/kafka_consumers".clear()
      file"$logsDir/kafka_success/kafka_producers".clear()      
   }
}