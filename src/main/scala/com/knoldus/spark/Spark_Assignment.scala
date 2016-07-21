package com.knoldus.spark

import org.apache.spark.{SparkContext, SparkConf}


object Spark_Assignment extends App {

  val sparkConf = new SparkConf().setMaster("local").setAppName("Spark_Assignment")
  val sparkContext = new SparkContext(sparkConf)

  println("\n!!!!!!!!!!!!!!!!!!!!!!!!!!! 1. Create RDD from the input file !!!!!!!!!!!!!!!!!!!!!!!!!!!\n")

  val pageCounts = sparkContext.textFile("/home/anurag/Desktop/pagecounts-20151201-220000")

  println("\n!!!!!!!!!!!!!!!!!!!!!!!!!!! 2. 10 records from the page counts !!!!!!!!!!!!!!!!!!!!!!!!!!!\n")

  pageCounts.take(10).foreach(println)
  val totalRecordInPageCounts = pageCounts.count()

  println("\n!!!!!!!!!!!!!!!!!!!!!!!!!!! 3. Total records in the page counts !!!!!!!!!!!!!!!!!!!!!!!!!!! " + totalRecordInPageCounts +"\n")

  println("\n!!!!!!!!!!!!!!!!!!!!!!!!!!! 4. Create RDD which containing only English pages from the page counts !!!!!!!!!!!!!!!!!!!!!!!!!!!\n")

  val enRDD = pageCounts.filter(totalPages => totalPages.split(" ").apply(0).contains("en"))
  val totalCountInEnRDD = enRDD.count()

  println("\n!!!!!!!!!!!!!!!!!!!!!!!!!!! 5. Total counts for the english pages !!!!!!!!!!!!!!!!!!!!!!!!!!! " + totalCountInEnRDD +"\n")

  val pagesReq = pageCounts.map(s => (s.split(" ").apply(1), s.split(" ").apply(2).toInt)).reduceByKey((a, b) => a + b).filter(_._2 > 200000).count()

  println("\n!!!!!!!!!!!!!!!!!!!!!!!!!!! 6. Pages that were requested more than 200,000 times in total !!!!!!!!!!!!!!!!!!!!!!!!!!! " + pagesReq +"\n")

  sparkContext.stop()
}