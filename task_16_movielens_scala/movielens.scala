package assignments

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object movielens {
  
  def main(args:Array[String]){
    //Initialize
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","MovieLens") 
    val sq = new org.apache.spark.sql.SQLContext(sc)
    import sq.implicits._
    
    //Define Schemas
                                  
    val itemSchema = StructType(Array(
                                    StructField("movie_id", IntegerType, true),
                                    StructField("movie_title", StringType, true),
                                    StructField("release_date", StringType, true),
                                    StructField("video_release_date", StringType, true),
                                    StructField("IMDb_URL", StringType, true),
                                    StructField("unknown", IntegerType, true),
                                    StructField("Action", IntegerType, true),
                                    StructField("Adventure", IntegerType, true),
                                    StructField("Animation", IntegerType, true),
                                    StructField("Children's", IntegerType, true),
                                    StructField("Comedy", IntegerType, true),
                                    StructField("Crime", IntegerType, true),
                                    StructField("Documentary", IntegerType, true),
                                    StructField("Drama", IntegerType, true),
                                    StructField("Fantasy", IntegerType, true),                                    
                                    StructField("Film-Noir", IntegerType, true),
                                    StructField("Horror", IntegerType, true),
                                    StructField("Musical", IntegerType, true),
                                    StructField("Mystery", IntegerType, true),
                                    StructField("Romance", IntegerType, true),
                                    StructField("Sci-Fi", IntegerType, true),
                                    StructField("Thriller", IntegerType, true),
                                    StructField("War", IntegerType, true),
                                    StructField("Western", IntegerType, true)))
                                  
    val userSchema = StructType(Array(
                                    StructField("user_id", IntegerType, true),
                                    StructField("age", IntegerType, true),
                                    StructField("gender", StringType, false),
                                    StructField("occupation", StringType, true),
                                    StructField("zip_code", StringType, true)))
    
    //Load Data    
                    
    val dataDF = sc.textFile("./data/ml-100k/u.data")
                    .map(row => row.replaceAll("\t", "|").split('|'))
                    .map(row => (row(0),row(1),row(2),row(3)))
                    .toDF("user_id","movie_id","rating","timestamp")
                    .selectExpr("cast(user_id as int) user_id",
                                "cast(movie_id as int) movie_id",
                                "cast(rating as int) rating",
                                "timestamp") 
                    .withColumn("timestamp", from_unixtime(col("timestamp")).cast(DateType))
                                          
    val itemDF = sq.read.format("csv").option("delimiter", "|")
                                        .option("header",false)
                                        .option("encoding", "ISO-8859-1")
                                        .schema(itemSchema)
                                        .load("./data/ml-100k/u.item")
                                        .drop("video_release_date","IMDb_URL")
                                        .withColumn("release_date",
                                          when(to_date(col("release_date"),"dd-MMM-yyyy").isNotNull,
                                            to_date(col("release_date"),"dd-MMM-yyyy"))
                                          .otherwise("Unknown Format").as("release_date"))
                                        
    val userDF = sq.read.format("csv").option("delimiter", "|")
                                        .option("header",false)
                                        .option("encoding", "ISO-8859-1")
                                        .schema(userSchema)
                                        .load("./data/ml-100k/u.user") 
                                        .drop("gender")
    
    //Create Name Map
    val nameMap:Map[Int, String] = itemDF.collect.map(row=>(row.getInt(0),row.getString(1))).toMap
    sc.broadcast(nameMap)
    
    //Create required intermediate RDDs
    val dataItemRDD = dataDF.drop("movie_title","user_id","timestamp")
                            .join(itemDF,"movie_id")
                            
    val dataUserRDD = dataDF.join(userDF,"user_id")
    
    val top10RDD = dataDF.groupBy("movie_id")
                            .count()
                            .orderBy(desc("count"))
                            .take(10)
    
    
    //Q1.Print names of the films with most reviews, unsorted. 
    println("\nQuestion 1:\n")                                      
    top10RDD.map(x=>nameMap.apply(x.getInt(0)))
              .foreach(println)
    
    // Q2.Print names of the films with most reviews, sorted alphabetically.
    println("\n\nQuestion 2:\n")       
    top10RDD.map(x=>nameMap.apply(x.getInt(0)))
              .sorted
              .foreach(println)
    
    // Q3.Print list of the number of ratings by genre
    println("\n\nQuestion 3:\n") 
    val dropList3 = Seq("movie_id","movie_title","release_date","rating")
    dataItemRDD.drop(dropList3:_*)
                .groupBy()
                .sum()
                .collect
                .flatMap(row => Map(dataItemRDD.drop(dropList3:_*)
                    .columns
                    .zip(row.toSeq):_*))
                .foreach(println)
              
    // Q4.Print oldest movie with a 5 rating.
    println("\n\nQuestion 4:\n")
    dataItemRDD.filter("rating = 5")
                .dropDuplicates()
                .select("movie_id","release_date")
                .orderBy(asc("release_date"))
                .take(1)
                .map(x=>nameMap.apply(x.getInt(0)))
                .foreach(println)
    
    // Q5.Print a list of the genre of the top 10 most rated movies.
    println("\n\nQuestion 5:\n")  
    
    //create list of only genre column names
    val filterList5 = itemDF.drop("movie_id","movie_title","release_date").columns
    
    //create UDF to produce genre list
    val genreString = udf((row: Row) => 
      row.getValuesMap[Int](row.schema.fieldNames).toList.filter(_._2 == 1).map(x => x._1))   
     
    itemDF.filter(col("movie_id").isin((top10RDD.map(row => row.getInt(0))):_*))
            .withColumn("genres", genreString(struct(filterList5 map col: _*)))
            .select("movie_title","genres")
            .show(truncate=false)
    
    // Q6.Print the title of the movie that was rated the most by students
    println("\n\nQuestion 5:\n")              
    dataUserRDD.filter(col("occupation") === "student")
                .groupBy("movie_id")
                .count()
                .orderBy(desc("count"))
                .take(1)
                .map(x=>nameMap.apply(x.getInt(0)))
                .foreach(println)
    
    // Q7.Print the list of movies that received the highest number of '5' rating 
    println("\n\nQuestion 7:\n")                  
    dataDF.filter(col("rating") === 5)
          .groupBy("movie_id")
          .count()
          .orderBy(desc("count"))
          .take(10) // not specified but seems an appropriate limit
          .map(x=>nameMap.apply(x.getInt(0)))
          .foreach(println)          
    
    // Q8.Print the list of zip codes corresponding to the highest number of users that rated movies.
    println("\n\nQuestion 8:\n")            
    userDF.groupBy("zip_code")
          .count()
          .orderBy(desc("count"))
          .take(10) // not specified but seems an appropriate limit
          .foreach(println)
    
    // Q9.Find the most rated movie by users in the age group 20 to 25.
    println("\n\nQuestion 9:\n")
    dataUserRDD.filter(col("age") <= 25 && col("age") >= 20 )
                .groupBy("movie_id")
                .count()
                .orderBy(desc("count"))
                .take(1)
                .map(x=>nameMap.apply(x.getInt(0)))
                .foreach(println)
    
    // Q10.Print the list of movies that were rated after year 1960.
    println("\n\nQuestion 10:\n")
    dataDF.filter(col("timestamp").gt(lit("1960-12-31")))
          .select("movie_id")
          .take(10) // not specified but seems an appropriate limit. Also could order by rating date but not asked.
          .map(x=>nameMap.apply(x.getInt(0)))
          .foreach(println)
    
    // Bonus. Convert UnixTimeStamp in timestamp column of u.data to human readable date 
    println("\n\nBonus Question:\n")
    dataDF.select("timestamp")
          .take(1)
          .foreach(println)
    
  }
}