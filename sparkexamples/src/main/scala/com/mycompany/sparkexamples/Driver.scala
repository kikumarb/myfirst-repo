package com.mycompany.sparkexamples
import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.functions._
import org.apache.log4j._

object Driver {
    def main(args:Array[String]){    
    println("hello world")
    //Logger.getLogger("org").setLevel(Level.ERROR)
    //Logger.getLogger("akka").setLevel(Level.WARN)
    //Log4JLogger logg = new Log4JLogger()
    //Logger.getLogger("Driver").setLevel(Level.INFO)
    
    //Logger log = new org.apache.log4j.Logger()
    
    val log = Logger.getLogger(this.getClass)
    
    log.setLevel(Level.INFO)
    
    log.warn("program started kishore")
    
    if(log.isInfoEnabled()){
      
      log.info("i will log everything")
      
    }
    
    val p = new Properties()
    val spark = SparkSession.builder.master("local[*]").appName("wordcount").enableHiveSupport().getOrCreate()
    
    //spark.sparkContext.setLogLevel("ERROR")
    
     
    val firstDF = spark.read.textFile("hdfs://quickstart.cloudera/user/cloudera/project/inputfiles/Sportswithhead.csv").rdd
    val firstDFheader = firstDF.first()
    
    
    
    val firstwoh = firstDF.filter(firstDF => (firstDF != firstDFheader))
        //contains(firstDFheader))
    //val secondDF = firstwoh.map(lines => lines.split(",")).filter(lines[))
    
    firstwoh.foreach(println)
    
    spark.stop() 
    
    }
    
    
    
    
    //val secondDF = firstDF.repartition(2)
    //val num = firstDF.repartition(3).getNumPartitions
    //val num2 = secondDF.getNumPartitions
    //println(num)
    //println(num2)
    
    ///val firstDF = spark.sql("select * from my_table")
    //val firstDF1 = spark.sql("select name,age from my_table").show() 
   //val gender = when(firstDF.col("name").equalTo("kishore"), "male").otherwise("female")
    //val firstDF1 = firstDF.withColumn("gender", gender)
    //val output = "hdfs://quickstart.cloudera/user/cloudera/project/hivetables/my_table"
    //firstDF1.write.csv("hdfs://quickstart.cloudera/user/cloudera/project/hivetables/my_table/details.csv")
    //firstDF1.write.mode("append").partitionBy("gender").parquet(output)
   //val myDF = spark.read.parquet("hdfs://quickstart.cloudera/user/cloudera/project/hivetables/my_table/gender=male")
   
   //myDF.show()
   //myDF.select("name").show()
    
   //firstDF1.show()
      
  //}
}