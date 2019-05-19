package com.neu.dataprocess.util

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaToHdfs {
  val kafkaServer : String = "58.87.124.238:9092"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("kafkaToHDFS").setMaster("local[*]")
    sparkConf.set("spark.streaming.backpressure.enabled", "true")
    sparkConf.set("spark.sql.parquet.compression.codec", "snappy")
    sparkConf.set("spark.sql.parquet.mergeSchema", "true")
    sparkConf.set("spark.sql.parquet.binaryAsString", "true")

    val sparkContext = new SparkContext(sparkConf)
//    val sqlContext = new SQLContext(sparkContext)
    val ssc = new StreamingContext(sparkContext, Seconds(10))
  }
}
