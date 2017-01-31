package com.datio.streaming


import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datio.streaming.Output.ParquetConnSettings
import com.datio.streaming.Structure.AvroMessage
import io.confluent.kafka.serializers.KafkaAvroDecoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord


import scala.io.Source

object StreamProcessor {

  def main(args: Array[String]): Unit = {

    implicit val conf = ConfigFactory.load
    val schemaString = Source.fromURL(getClass.getResource("/avro-schema.avsc")).mkString
    val schema = new Schema.Parser().parse(schemaString)
    val sparkConf = new SparkConf()
//      .setMaster(conf.getString("spark.master"))
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
      "schema.registry.url" -> conf.getString("kafka.schema.registry.url"),
      "group.id" -> conf.getString("kafka.group.id"))

    val parquetSettings = ParquetConnSettings(sqlContext)
    val operations = Operations.Operations(sqlContext)


    val topic = conf.getString("kafka.topics")
    val topicSet = Set(topic)
    val kafkaStream = KafkaUtils
      .createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaParams, topicSet).map(_._2)



    kafkaStream.map(value => {
      AvroMessage(value.asInstanceOf[GenericRecord].get("id").toString.toInt,
        value.asInstanceOf[GenericRecord].get("name").toString,
        value.asInstanceOf[GenericRecord].get("surname").toString,
        value.asInstanceOf[GenericRecord].get("age").toString.toInt)

    }).foreachRDD(rdd => {
      import sqlContext.implicits._

      if(!rdd.isEmpty()){
        val df = rdd.toDF()
        println("*********************************************")
        df.show
        println("*********************************************")
        println("*********************************************")
//        val groupedDf = operations.group(df)
        parquetSettings.saveDf(df)
        println("*********************************************")
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
