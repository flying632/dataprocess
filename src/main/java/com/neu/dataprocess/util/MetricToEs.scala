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

case class CpuMetric(timestamp:String,host:Host,cpu:CPU)
case class DiskMetric(timestamp:String,host:Host,diskio:DiskIO)
case class FileMetric(timestamp:String,host:Host,filesystem:FileSystem)
case class MemoryMetric(timestamp:String,host:Host,memory: Memory)
case class LoadMetric(timestamp:String,host:Host,load:Load)
//,cpu:CPU,diskio:DiskIO,filesystem:FileSystem,memory:Memory,load:Load
case class Host(name:String,architecture:String,os:OS)
case class OS(platform:String,version:String,family:String,name:String)
case class CPU(cores:String,systemPct:String,userPct:String,totalPct:String)
case class DiskIO(name:String,readBytes:String,writeBytes:String)
case class FileSystem(device:String,free:String,total:String,usedPct:String,usedBytes:String)
case class Memory(free:String,total:String,swapTotal:String,usedBytes:String,usedPct:String)
case class Load(one:String,five:String,fifteen:String)
object MetricToEs {
  val kafkaServer : String = "58.87.124.238:9092"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MetricToES").setMaster("local[*]")
    //设置es的相关参数
    conf.set("es.nodes", "datanode")
    conf.set("es.port", "9200")
    conf.set("es.nodes.wan.only","true")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))
    val props: Properties = getBasicStringStringConsumer();
    val topic = "test"

    val kafkaStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](
        util.Arrays.asList(topic),
        props.asInstanceOf[java.util.Map[String,Object]]
      )
    )
    kafkaStream.foreachRDD(r => {

      val ESRdd = r.map(x=>{
        println(x.value())
        val value = JSON.parseObject(x.value())
        val timestamp = value.get("@timestamp").toString
        val hostObject = JSON.parseObject(value.get("host").toString)
        val osObject = JSON.parseObject(hostObject.get("os").toString)
        val os = OS(osObject.get("platform").toString,osObject.get("version").toString,
          osObject.get("family").toString,osObject.get("name").toString)
        val host = Host(hostObject.get("name").toString,hostObject.get("architecture").toString,os)
        val systemObject = JSON.parseObject(value.get("system").toString)

        var result:Object = null
        if (systemObject.get("cpu") != null) {
          val cpuObject = JSON.parseObject(systemObject.get("cpu").toString)
          val cores = cpuObject.get("cores").toString
          val userPct = JSON.parseObject(cpuObject.get("user").toString).get("pct").toString
          val systemPct = JSON.parseObject(cpuObject.get("system").toString).get("pct").toString
          val totalPct = JSON.parseObject(cpuObject.get("total").toString).get("pct").toString
          result = CpuMetric(timestamp,host,CPU(cores,systemPct,userPct,totalPct))
        }

        if (systemObject.get("diskio") != null) {
          val diskioObject = JSON.parseObject(systemObject.get("diskio").toString)
          val name = diskioObject.get("name").toString
          val readBytes = JSON.parseObject(diskioObject.get("read").toString).get("bytes").toString
          val writeBytes = JSON.parseObject(diskioObject.get("write").toString).get("bytes").toString
          result = DiskMetric(timestamp,host,DiskIO(name,readBytes,writeBytes))
        }
        if (systemObject.get("filesystem") != null) {
          val fileSystemObject = JSON.parseObject(systemObject.get("filesystem").toString)
          val deviceName = fileSystemObject.get("device_name").toString
          val total = fileSystemObject.get("total").toString
          val free = fileSystemObject.get("free").toString
          val usedObject = JSON.parseObject(fileSystemObject.get("used").toString)
          val usedBytes = usedObject.get("bytes").toString
          val usedPct = usedObject.get("pct").toString
          result = FileMetric(timestamp,host,FileSystem(deviceName,free,total,usedPct,usedBytes))
        }
        if (systemObject.get("load") != null) {
          val loadObject = JSON.parseObject(systemObject.get("load").toString)
          val one = loadObject.get("1").toString
          val five = loadObject.get("5").toString
          val fifteen = loadObject.get("15").toString
          result = LoadMetric(timestamp,host,Load(one,five,fifteen))
        }
        if (systemObject.get("memory") != null) {
          val memoryObject = JSON.parseObject(systemObject.get("memory").toString)
          val free = memoryObject.get("free").toString
          val total = memoryObject.get("total").toString
          val usedObject = JSON.parseObject(memoryObject.get("used").toString)
          val usedPct = usedObject.get("pct").toString
          val usedBytes = usedObject.get("bytes").toString
          val swapTotal = JSON.parseObject(memoryObject.get("swap").toString).get("total").toString
          result = MemoryMetric(timestamp,host,Memory(free,total,swapTotal,usedBytes,usedPct))
        }
        result
      })

      import org.elasticsearch.spark._
      ESRdd.saveToEs("/test2/metric")

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
