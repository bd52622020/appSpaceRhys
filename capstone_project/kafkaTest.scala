package radio

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame,Row}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.Trigger


object kafkaTest {
  def main(args:Array[String]) = {
    
    val SuccessLogs:String = s"${args(0)}/kafka_success/*/*"

    
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
                                    .csv(SuccessLogs)
                                    .groupBy(window(col("timestamp"),"180 seconds","60 seconds"),col("message"))
                                    .agg(count(col("message")).as("count"))
                                    .filter(col("count") === 1)
                                    .writeStream
                                    .queryName("failed")
                                    .outputMode("complete")
                                    .format("memory")
                                    .start()
 
    //displays messages that were not paired in three consecutive windows                               
    while(logs.isActive){
      Thread.sleep(5000)
      spark.sql("select * from failed")
           .groupBy("message")
           .count
           .filter(col("count") === 3)
           .drop("count")
           .show(truncate=false) 
    
    }
    //cleanup                                                              
    logs.awaitTermination()
    logs.stop()
  }
}