package logic

import java.util.Properties
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, expr, lit}
import logic.ReusableFunctions._


object Test extends App with SparkConfigs {

 // print("Hello")

  // create DataFrame from scala Seq. It can infer schema for you.
  val df1 = sparkSession.createDataFrame(Seq((-1, "andy", 20, "USA"), (2, "jeff", 23, "China"), (3, "james", -18, "USA"))).toDF("id", "name", "age", "country")
  // df1.printSchema
  // df1.show()

  val columnNames = List("id", "age")

  val (sdf, edf) = filterRecordsHavingNegativeValue (df1, columnNames)

  sdf.show()
  edf.show()

  // df1.filter(col("id") < 0).show
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
