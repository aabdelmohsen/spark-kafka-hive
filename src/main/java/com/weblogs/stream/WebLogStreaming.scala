package com.weblogs.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.control.Exception.Catch

object WebLogStreaming {

  def main(args: Array[String]) {

    val confg = new SparkConf().setAppName("WebLogsStreaming").setMaster("local[2]")
    val sc = new SparkContext(confg)
    val ssc = new StreamingContext(sc, Seconds(3))
    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "group.id" -> "spark-streaming-notes",
      "auto.offset.reset" -> "smallest")

    val topics = Array("WebLogs").toSet

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    stream.map(_._2).foreachRDD(processRDD(_, sc))

    ssc.start()
    ssc.awaitTermination()
  }

  def processRDD(rdd: RDD[String], sc: SparkContext) {
    val values = rdd.flatMap(e => e.split(",")).collect()
    
    // Ensuring that the RDD is not empty and each array has 4 values after split
    // and then check that the last value is numeric which represent that the request done successfully and contains status code 
    if (!values.isEmpty && values.size == 4) {
      if (isNumeric(values(3))) {
        var ip = values(0)
        var dateTime = values(1)
        var requestType = values(2).split(" ")(0)
        var requestPage = values(2).split(" ")(1)
        var responseStatus = values(3)
        println(ip + ":" + dateTime + ":" + requestType + ":" + requestPage + ":" + responseStatus)
        saveLogToHive(sc, ip, dateTime, requestType, requestPage, responseStatus)
      }
    }
  }

  def isNumeric(input: String): Boolean = input.forall(_.isDigit)

  def saveLogToHive(sc: SparkContext, ip: String, datetime: String, reqType: String, page: String, status: String) {

    println("============== Start Saving Log Record  ================")
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("CREATE TABLE IF NOT EXISTS WEBLOGS (ip STRING, datetime STRING, type STRING, page STRING, status STRING)")
    var sql: String = "select '" + ip + "' as ip, '" + datetime + "' as datetime, '" + reqType + "' as type, '" + page + "' as page, '" + status + "' as status"
    val data = hiveContext.sql(sql)
    data.write.mode("append").saveAsTable("WEBLOGS")
    println("============== Record Saved Successfully =================")
    
  }

}