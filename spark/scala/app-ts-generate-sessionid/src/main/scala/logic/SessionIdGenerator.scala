package logic

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame,SparkSession}
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.udf

import java.text.SimpleDateFormat
import org.joda.time.Seconds

object SessionIdGenerator {

  val log = Logger.getLogger(SessionIdGenerator.getClass)

  def process(spark:SparkSession): Unit = {

    //Creating dataframe from a csv file location
    val rawDataframe = spark.read.format("csv").option("header", "true").load("src/main/resources/source")
    log.info("Created Raw dataframe.")
    log.debug("Raw dataframe contents."+rawDataframe.collect())

    //Calling getSessionIds method to create a dataframe with session_id and activity_time
    //Adding columns year,month and date.Will be used for partitioning
    val finalDataframe = getSessionIds(rawDataframe)
      .withColumn("yr",year(col("click_ts")))
      .withColumn("mm",month(col("click_ts")))
      .withColumn("dd",dayofmonth(col("click_ts")))
    log.info("Added partition columns to Dataframe.")

    log.info("Writing the dataframe to sink in parquet format partitioned by \"yr\",\"mm\",\"dd\"")
   //Writing to sink in parquet format
    finalDataframe.coalesce(1).
      write.partitionBy("yr","mm","dd").mode("overwrite").parquet("src/main/resources/sink")

    //Read partitioned parquet data from sink and create dataframe
    val df_parquet = spark.read.parquet("src/main/resources/sink")

   //Creating a temporary view on a Dataframe
    df_parquet.createOrReplaceTempView("user_clicks_info")

    //Spark sql queries to calculate timeSpentInMonth and timeSpentInDay for a user_id
    val timeSpentInMonth = spark.sql("SELECT user_id,yr,mm,sum(activity_time) FROM user_clicks_info GROUP BY user_id,yr,mm")
    val timeSpentInDay = spark.sql("SELECT user_id,yr,mm,dd,sum(activity_time) FROM user_clicks_info GROUP BY user_id,yr,mm,dd")

    timeSpentInMonth.show(false)
    timeSpentInDay.show(false)
  }

  //Function to generate session id.Will be used in UDF
  def generateSessionId(userId: String, clickList: Seq[String], tsList: Seq[Long]) = {
    val tmo1 = 60 * 60 //timeout1
    val tmo2 = 2 * 60 * 60 //timeout2
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val clickListInDate = clickList.map(s => new DateTime(dateFormat.parse(s)))
    var currentSessionStartTime = clickListInDate(0)
    var sessionStartTime = Seq(currentSessionStartTime)

    val totalElements = clickList.length
    for (i <- 1 to totalElements - 1) {
      if (tsList(i) >= tmo1) {
        currentSessionStartTime = clickListInDate(i)
      }
      else {
        val timeDiff = Seconds.secondsBetween(currentSessionStartTime, clickListInDate(i)).getSeconds
        if (timeDiff >= tmo2) {
          currentSessionStartTime = clickListInDate(i)
        }
      }
      sessionStartTime = sessionStartTime :+ currentSessionStartTime
    }
    val activityTimeList = tsList.map(x=> if(x > tmo1) 0 else x)
    ((sessionStartTime.map(x => x.toString + userId) zip clickList) zip activityTimeList).map(x=>(x._1._1,x._1._2,x._2))
  }

  def getSessionIds(dataFrame: DataFrame):DataFrame =
  {
    //Adding two columns ts_converted and ts_lag
    val dataframeWithClickTimeAsTimestamp = dataFrame
      .withColumn("ts_converted", to_timestamp(col("click_ts"), "yyyy-MM-dd HH:mm:ss")) //Typecasting string to timestamp
      .withColumn("ts_lag", lag(col("ts_converted"), 1) //Lag column
        .over(Window.partitionBy(col("user_id")).orderBy("click_ts")))
    log.info("Added two columns ts_converted and ts_lag to existing dataframe.")

    //Adding a column ts_diff (time difference of current click time and previous click time)
    val dataframeWithTimeDifferenceColumn = dataframeWithClickTimeAsTimestamp.withColumn("ts_diff",
      (unix_timestamp(col("ts_converted")) - unix_timestamp(col("ts_lag"))))
      .withColumn("ts_diff", when(col("ts_diff").isNull, 0).otherwise(col("ts_diff")))
    log.info("Added column ts_diff to existing dataframe.")

    //GROUP BY on user_id and creating lists for click_ts and ts_diff
    val dataframeWithAggregatedValues = dataframeWithTimeDifferenceColumn.groupBy("user_id").
      agg(
        collect_list(col("click_ts")).as("click_ts_list"),
        collect_list(col("ts_diff")).as("ts_diff_list")
      )
    log.info("Group by on user_id column and generated click_ts_list & ts_diff_list column.")

    //UDF definition for generating unique session ids
    val generateSessionId_UDF = udf[Seq[(String, String, Long)], String, Seq[String], Seq[Long]](generateSessionId)

    //Creating session_id column (i.e. MD5 hash) and activity_time column
    val finalDataframeWithSessionID = dataframeWithAggregatedValues
      .withColumn("session_id_and_click_time",
        explode(generateSessionId_UDF(col("user_id"),
          col("click_ts_list"), col("ts_diff_list"))))
      .select(col("user_id"),
        col("session_id_and_click_time._1").as("session_id"),
        col("session_id_and_click_time._2").as("click_ts"),
        col("session_id_and_click_time._3").as("activity_time"))
      .withColumn("session_id", md5(col("session_id")))
    log.info("Dataframe with session_id,click_ts,activity_time generated.")

    finalDataframeWithSessionID

  }

}