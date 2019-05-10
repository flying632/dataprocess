package com.neu.dataprocess.util

import java.util.Date
import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


case class Message(timestamp: String, source: String, date:String, host: String, serviceName: String, log: String)

object KafkaUtil {
  val kafkaServer : String = "58.87.124.238:9092"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SimpleStreaming").setMaster("local[*]")
    //设置es的相关参数
    conf.set("es.nodes", "datanode")
    conf.set("es.port", "9200")
    conf.set("es.nodes.wan.only","true")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))
    val props: Properties = getBasicStringStringConsumer();
    val topic = "test2"

    val kafkaStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](
        util.Arrays.asList(topic),
        props.asInstanceOf[java.util.Map[String,Object]]
      )
    )
    println("*** begin")
    kafkaStream.foreachRDD(r => {

      val ESRdd = r.map(x=>{
        val value = JSON.parseObject(x.value())
        val timestamp = value.get("@timestamp").toString
        val source = value.get("source").toString
        val message = value.get("message").toString
        val messageArray = message.split(" ")
//        val fm = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
        val date =  messageArray.apply(0)+" "+messageArray.apply(1)
        var msg = Message("May 9th 2019, 21:48:53.316","defaultsource","May 9th 2019, 21:48:53.316","default","default","wrong log format")
        if (messageArray.size >= 5) {
          var i = 4
          var log = ""
          while (i<messageArray.size){
            log = log + " " + messageArray.apply(i)
            i = i + 1
          }
          msg = Message(timestamp,source,date,messageArray.apply(2),messageArray.apply(3),log)
        }
//        println("6666 " + msg.host + " " + msg.log + "  "+ msg.serviceName)
        msg
      })

      import org.elasticsearch.spark._
      ESRdd.saveToEs("/test1/message")

    })

    ssc.start()

    println("*** started termination monitor")

    // streams seem to need some time to get going
//    Thread.sleep(5000)
    ssc.awaitTermination()
  }
  def getBasicStringStringConsumer(group:String = "MyGroup") : Properties = {
    val consumerConfig: Properties = new Properties
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer )
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    //consumerConfig.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "roundrobin")

    consumerConfig
  }


}
