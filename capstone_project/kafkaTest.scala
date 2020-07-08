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
    
    //val w = Window.partitionBy("topic", "partition", "offset").orderBy(colName, colNames)
    val logs = spark.readStream.option("sep", ",")
                                    .option("header", "false") 
                                    .schema(schema)
                                    .csv(SuccessLogs)
                                    .filter(col("timestamp") <= (current_timestamp() - expr("INTERVAL 30 SECONDS")))                                    
                                    .withWatermark("timestamp", "30 seconds") //how long to wait before dropping late data
                                    .groupBy(window(col("timestamp"),"24 hours"),col("message"))
                                    .agg(count(col("message")).as("count"))
                                    .filter(col("count") === 1)
                                    .writeStream
                                    .queryName("failed")
                                    .outputMode("update")    
                                    .format("console")
                                    .option("truncate", false)
                                    .start
                                                                  
    logs.awaitTermination()
    
    //every x seconds remove producer logs with matching consumer logs. Check remainder. If age above certain value, print to log, then delete.
  }
}
