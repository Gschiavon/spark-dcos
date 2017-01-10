import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSpec, TestSuite, WordSpec}

case class Person(firstName: String, lastName: String, birthDate: Int)

class DataFrameOp extends WordSpec {

  "test dataframes operations" in {
    val sc = SparkContext.getOrCreate(new SparkConf().setMaster("local[*]").setAppName("test"))
    val sqlCtx = SQLContext.getOrCreate(sc)

    val peopleDF = sqlCtx.createDataFrame(Seq(
      Person("Mario", "Delrio", 123456),
      Person("German", "Delatorre", 234567),
      Person("Sergio", "Garcia", 897654),
      Person("Iñaki", "Delmar", 765894),
      Person("Carlos", "Garcia", 347690),
      Person("Mario", "Delrio", 164533)))

    val df = peopleDF.groupBy("firstName").count
    val sortedDf = df.sort(df("count").desc).show

  }
}
