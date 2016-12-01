package com.datio.streaming.Output

import java.util.Properties

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


case class PostgresOutput (SQLContext: SQLContext) {

  val postgresSchema = new StructType(
    Array(
      StructField("firstName", StringType, false),
      StructField("lastName", StringType, false)))

  def save(rdd: RDD[Row])(implicit conf: Config): Unit = {
    val jdbcURL = conf.getString("jdbc.url")
    val tableName = conf.getString("table")
    val user = conf.getString("user")
    val password = conf.getString("password")
    val df = SQLContext.createDataFrame(rdd, postgresSchema)
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", user)
    connectionProperties.setProperty("password", password)
    connectionProperties.setProperty("driver", "org.postgresql.Driver")
    df.write.mode(Append).jdbc(jdbcURL, tableName, connectionProperties)

  }
}
