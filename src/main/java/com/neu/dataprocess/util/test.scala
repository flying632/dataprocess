package com.neu.dataprocess.util

import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

case class Message1(host: String)

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("iteblog").setMaster("local[*]")
    conf.set("es.nodes", "datanode")
    conf.set("es.port", "9200")
    conf.set("es.nodes.wan.only","true")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")
    val message1 = Message1("123")
    val message2 = Message1("234")
    import org.elasticsearch.spark._
    val esRdd = sc.makeRDD(Seq(message1,message2))
    
    esRdd.saveToEs("iteblog/docs")
  }
}
