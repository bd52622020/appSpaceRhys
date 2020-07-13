import org.scalatest.funsuite.AnyFunSuite
import radio.textConsumer.{jsonParse,kafkaStreamInit,computeStream}
import org.json4s.JsonInput
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecord}
import org.apache.kafka.common.serialization.{StringSerializer,StringDeserializer}
import java.util.Arrays
import java.util.Properties
import org.apache.kafka.clients.admin.{AdminClient,AdminClientConfig}
import org.apache.kafka.clients.admin.CreateTopicsResult
import org.apache.kafka.clients.admin.NewTopic
import java.time.Instant
import org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream
import org.apache.spark.streaming.kafka010.{LocationStrategies,ConsumerStrategies}
import org.bson.Document
import org.bson.BsonArray.parse

class SetSuite extends AnyFunSuite {

  test("jsonParse should return tuple containing correct timestamp/transcript information from json") {
    val parsedJson = jsonParse("""{"timestamp":"11111","transcript":"bar"}""")
    assert(parsedJson._1 == "11111")
    assert(parsedJson._2 == "bar")
    
  }
  
  test("computeStream should take InputDStream[ConsumerRecord[String, String]] and return corresponding DStream[Document]"){
    
    val properties = new Properties()
    val topicString = Instant.now.getEpochSecond.toString() + "_testci"
    properties.setProperty("bootstrap.servers", "data-VirtualBox:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("acks","all")
    val adminProps = new Properties()
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "data-VirtualBox:9092")  
    adminProps.put(AdminClientConfig.CLIENT_ID_CONFIG, topicString)
    val serializer = new StringSerializer  
    val localKafkaAdmin = AdminClient.create(adminProps)
    localKafkaAdmin.createTopics(Arrays.asList(new NewTopic(topicString, 1, 1.toShort)))
    localKafkaAdmin.close()
    val producer: KafkaProducer[String, String] = new KafkaProducer(properties)
    producer.send(new ProducerRecord[String, String](topicString, "key", """{"timestamp":"11111","transcript":"foo bar"}"""))
    producer.close()
    
    val sConf:SparkConf = new SparkConf().setMaster("local[*]").setAppName(topicString)                               
    val ssc:StreamingContext = new StreamingContext(sConf, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")
    var kafkaConfig = Map[String, Object](
                "bootstrap.servers" -> "data-VirtualBox:9092",
                "key.deserializer" -> classOf[StringDeserializer],
                "value.deserializer" -> classOf[StringDeserializer],
                "group.id" -> topicString,
                "auto.offset.reset" -> "earliest",
                "enable.auto.commit" -> (true: java.lang.Boolean))
                
    val dStream = createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(topicString), kafkaConfig))   
        
    val outDStream = computeStream(dStream)                 
    outDStream.foreachRDD(rdd=> rdd.foreach(record => {
          assert(record.isInstanceOf[Document]);
          assert(record.get("timestamps").toString() == "[11111]");
          assert(record.get("station") == "key");
          assert(record.get("raw_text") == "foo bar")}))
          
    ssc.start() // start the computation
    ssc.awaitTerminationOrTimeout(5000)
    ssc.stop(true, true)    
  }
  
  test("kafkaStreamInit should return an inputDStream containing messages in provided topics"){
    
    val topicString = Instant.now.getEpochSecond.toString() + "_testsi"
    val properties = new Properties()
    properties.put("bootstrap.servers", "data-VirtualBox:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("acks","all")
    val adminProps = new Properties()
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "data-VirtualBox:9092")
    adminProps.put(AdminClientConfig.CLIENT_ID_CONFIG, topicString)
    val serializer = new StringSerializer
    val localKafkaAdmin = AdminClient.create(adminProps)
    localKafkaAdmin.createTopics(Arrays.asList(new NewTopic(topicString, 1, 1.toShort)))
    localKafkaAdmin.close()
    val producer: KafkaProducer[String, String] = new KafkaProducer(properties)
    producer.send(new ProducerRecord[String, String](topicString, "key", """{"timestamp":"11111","transcript":"foo bar"}"""))
    producer.close()
    
    val sConf:SparkConf = new SparkConf().setMaster("local[*]").setAppName(topicString)                               
    val ssc:StreamingContext = new StreamingContext(sConf, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")
    var kafkaConfig = Map[String, Object](
                "bootstrap.servers" -> "data-VirtualBox:9092",
                "key.deserializer" -> classOf[StringDeserializer],
                "value.deserializer" -> classOf[StringDeserializer],
                "group.id" -> topicString,
                "auto.offset.reset" -> "earliest",
                "enable.auto.commit" -> (true: java.lang.Boolean))
                
    val dStream = kafkaStreamInit(ssc,topicString,kafkaConfig)
    dStream.foreachRDD(rdd=> rdd.foreach(record =>
      assert(record.key == "key" && record.value == """{"timestamp":"11111","transcript":"foo bar"}""")))
      
    ssc.start() // start the computation
    ssc.awaitTerminationOrTimeout(5000)
    ssc.stop(true, true)
  }
} 