package com.datio.streaming


import com.datio.streaming.Commons.Commons._
import com.typesafe.config.ConfigFactory
import kafka.serializer._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datio.streaming.Output.ParquetConnSettings


object StreamProcessor {

  def main(args: Array[String]): Unit = {

    implicit val conf = ConfigFactory.load

    val sparkConf = new SparkConf()
      .setMaster(conf.getString("spark.master"))
      .setAppName(conf.getString("spark.appName"))

    sparkConf.set("spark.streaming.backpressure.enabled", "true")
    sparkConf.set("spark.sql.parquet.compression.codec", "snappy")
    sparkConf.set("spark.sql.parquet.mergeSchema", "true")
    sparkConf.set("spark.sql.parquet.binaryAsString", "true")

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val ssc = new StreamingContext(sparkContext, Seconds(10))

    val kafkaParams = Map(
    "metadata.broker.list" -> conf.getString("kafka.metadata.broker.list"),
    "group.id" -> conf.getString("kafka.group.id"))

    val parquetSettings = ParquetConnSettings(sqlContext)
    val operations = Operations.Operations(sqlContext)


    val topic = conf.getString("kafka.topics")
    val topicSet = Set(topic)
    val kafkaStream = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet).map(_._2)


    kafkaStream.map(value => {
      val jsonParsed = parseJSONField(value)
      toRow(jsonParsed)
    }).foreachRDD(rdd => {
      if(!rdd.isEmpty()){
//      parquetSettings.save(rdd)
       parquetSettings.saveDf(operations.group(operations.createDF(rdd)))

      }
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
