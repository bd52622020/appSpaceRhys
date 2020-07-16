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



class IntegrationTests extends AnyFunSuite {
    
   test("pipeline should publish valid bson document to mongo from audio stream") {
     
     val pythonScriptsDir = "/home/data/project"
     
     val p1 = s"python3 $pythonScriptsDir/input.py teststation http://bbcmedia.ic.llnwd.net/stream/bbcmedia_radio5live_mf_p.m3u".run()
     val p2 = s"python3 $pythonScriptsDir/transcript.py teststation test_transcripts".run()
     
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
     println(record)
     println(record.get("timestamps").toString())
     assert(record.get("station").toString() == "teststation")
     assert(record.get("raw_text").toString().split(" ").contains("the"))
     assert("[0-9]{13}".r.findAllIn(record.get("timestamps").toString()).length > 0)
     assert("the=[1-9]".r.findAllIn(record.get("tokenized_text").toString()).length == 1)
   } 
}