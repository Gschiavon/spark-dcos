package com.datio.streaming.Operations

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


case class Operations(SQLContext: SQLContext) {


	implicit val dataFrameSchema = new StructType(
		Array(
			StructField("firstName", StringType, false),
			StructField("lastName", StringType, false),
			StructField("date", StringType, false)))

	def createDF(rdd: RDD[Row]): DataFrame = {
		SQLContext.createDataFrame(rdd, dataFrameSchema)
	}

	def group(df: DataFrame): DataFrame = {
		val df1 = df.groupBy("name").count
		val dfSorted = df1.sort(df1("count").desc)
		dfSorted
	}
}


