import org.scalatest.funsuite.AnyFunSuite
import radio.textConsumer.{jsonParse,kafkaStreamInit,streamFromKafka}
import org.json4s.JsonInput
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecord}
import net.manub.embeddedkafka.{EmbeddedKafka,EmbeddedKafkaConfig}
import net.manub.embeddedkafka.Codecs.stringSerializer
import org.apache.kafka.common.serialization.{StringSerializer,StringDeserializer}
import java.util.Properties
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.CreateTopicsResult
import org.apache.kafka.clients.admin.NewTopic
import java.time.Instant


class SetSuite extends AnyFunSuite with EmbeddedKafka {

  test("jsonParse should return tuple containing correct timestamp/transcript information from json") {
    val parsedJson = jsonParse("""{"timestamp":"foo","transcript":"bar"}""")
    assert(parsedJson._1 == "foo")
    assert(parsedJson._2 == "bar")
  }
  
  test("getMongoConfig should return valid mongoConfig"){
    
  }
  
  test("getSparkConf should return valid sparkConf"){
  }  
  
  test("getKafkaConf should return valid Map[String, Object])"){
    
  }

  test("kafkaStreamInit should return a valid InputDStream"){
    
  }
  test("messageReceiptLog should produce a logfile for a message received from kafka"){
    
  }
  test("computeStream should take InputDStream[ConsumerRecord[String, String]] and return DStream[Document]"){
    
  }
  test("kafkaStreamInit should return an inputDStream containing messages in provided topics"){
     
    val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 6001, zooKeeperPort = 6000)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:6001")
    properties.setProperty("zookeeper.connect", "localhost:6000")
    properties.setProperty("group.id", "sparkTestProducer")
    properties.setProperty("auto.offset.reset", "earliest")
    val serializer = new StringSerializer
    
    withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
        val topicString = Instant.now.getEpochSecond.toString() 
        val producer: KafkaProducer[String, String] = aKafkaProducer[String](serializer, userDefinedConfig)
        producer.send(new ProducerRecord[String, String](topicString, "key", """{"timestamp":"2020-07-09 16:38:29","transcript":"foo bar"}"""))
        producer.close()
        val sConf:SparkConf = new SparkConf().setMaster("local[*]").setAppName(topicString)                               
        val ssc:StreamingContext = new StreamingContext(sConf, Seconds(1))
        ssc.sparkContext.setLogLevel("ERROR")
        var kafkaConfig = Map[String, Object](
                    "bootstrap.servers" -> "localhost:6001",
                    "key.deserializer" -> classOf[StringDeserializer],
                    "value.deserializer" -> classOf[StringDeserializer],
                    "group.id" -> topicString,
                    "auto.offset.reset" -> "earliest",
                    "enable.auto.commit" -> (true: java.lang.Boolean))
                    
        val dStream = kafkaStreamInit(ssc,topicString,kafkaConfig)
        dStream.foreachRDD(rdd=> rdd.foreach(record => {println(record);
          assert(record.key == "key" && record.value == """{"timestamp":"2020-07-09 16:38:29","transcript":"foo bar"}""")}))
        ssc.start() // start the computation
        ssc.awaitTerminationOrTimeout(10000)
    }
  }
} 