package com.datio.streaming.Output

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

case class ParquetConnSettings(SQLContext: SQLContext) {

  val parquetSchema = new StructType(
  Array(
    StructField("firstName", StringType, false),
    StructField("lastName", StringType, false)))

  def save(rdd: RDD[Row])(implicit conf: Config): Unit = {
    val hdfsPath = conf.getString("hdfs.path")
    val df = SQLContext.createDataFrame(rdd, parquetSchema)
    df.show
    df.printSchema()
    df.write.mode(Append).parquet(hdfsPath)
  }

  def saveDf(dataFrame: DataFrame)(implicit conf: Config): Unit = {
    val hdfsPath = conf.getString("hdfs.path")
    dataFrame.write.mode(Append).partitionBy("age")parquet(hdfsPath)
  }
}
