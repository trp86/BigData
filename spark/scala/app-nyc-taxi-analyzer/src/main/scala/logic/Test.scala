package logic

import commons.SparkConfigs

import java.util.Properties
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, expr, lit}
import commons.ReusableFunctions


object Test extends App with SparkConfigs {

 // print("Hello")

  // create DataFrame from scala Seq. It can infer schema for you.
   val df1 = sparkSession.createDataFrame(Seq((-1, "andy", 20, "USA"), (2, "jeff", 23, "China"), (3, "james", -18, "USA"))).toDF("id", "name", "age", "country")
  // df1.printSchema
  // df1.show()

  // val (sdf, edf) = dataframeColumnsCompare(df1, List("id", "age"), ">")

 // sdf.show()
 // edf.show()

  // val columnNames = List("id", "age")

  // val x = sparkSession.read.format("csv").option("header", true).option("delimiter", ",").
    // load("""src/main/resources/inputdata/nyccitytaxi/yellow_tripdata_subset_2014-01.csv.gz""")

  // x.show()


  import org.apache.spark.sql.functions._
  import sparkSession.implicits._
  val dfDate = Seq(("07-01-2019 12:01:19"),
    ("2020-01-01 00:28:15"),
    ("06-24-2019 12 01 19 406"),
    ("11-16-2019 16 44 55 406"),
    ("11-16-2019 16 50 59 406")).toDF("input_timestamp")

  // dfDate.withColumn("datetype_timestamp", to_timestamp(col("input_timestamp"), "yyyy-MM-dd HH:mm:ss")).show(false)


  // val (sdf, edf) = filterRecordsHavingImproperDateTimeValue (dfDate, List("input_timestamp"))

  // sdf.show()
  // edf.show()

  // df1.filter(!(col("id") < 0)).show

  /* val exp = expr("""id < 0 or age < 0""")

  df1.filter("""id < 0 or age < 0""").show



  val emptyDF = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], df1.schema).withColumn("rejectReason", lit(""))

    val  b = columnNames.foldLeft(emptyDF) ((a, b) => {
       val x = col(b)<0
       a.union(df1.filter(x).withColumn("rejectReason", lit(b + " is negative")))
     })

     b.show()
*/


}
