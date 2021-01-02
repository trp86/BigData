package logic

import logic.SessionIdGenerator.generateSessionId
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class SessionIdGeneratorTest extends AnyFlatSpec{

  "Method generateSessionId" should "return sequence of tuples containing (sessionid,clicktime,activitytime) when user_id,clickList,tsList is passed" in {

    //given
    val user_id = "u1"
    val clickList: Seq[String] = Seq("2018-01-01 11:00:00",
      "2018-01-01 12:10:00",
      "2018-01-01 13:00:00",
      "2018-01-01 13:25:00",
      "2018-01-01 14:40:00",
      "2018-01-01 15:10:00",
      "2018-01-01 16:20:00",
      "2018-01-01 16:50:00")
    val tsList: Seq[Long] = Seq(0, 4200, 3000, 1500, 4500, 1800, 4200, 1800)

    //when
    val actualSequence = generateSessionId(user_id,clickList,tsList)
    val expectedSequence = Seq(
      ("2018-01-01T12:10:00.000+01:00u1","2018-01-01 12:10:00",0),
      ("2018-01-01T12:10:00.000+01:00u1","2018-01-01 13:00:00",3000),
      ("2018-01-01T12:10:00.000+01:00u1","2018-01-01 13:25:00",1500),
      ("2018-01-01T14:40:00.000+01:00u1","2018-01-01 14:40:00",0),
      ("2018-01-01T14:40:00.000+01:00u1","2018-01-01 15:10:00",1800),
      ("2018-01-01T16:20:00.000+01:00u1","2018-01-01 16:20:00",0),
      ("2018-01-01T16:20:00.000+01:00u1","2018-01-01 16:50:00",1800),
      ("2018-01-01T11:00:00.000+01:00u1","2018-01-01 11:00:00",0))

    //assert
    assertResult(expectedSequence.toSet) (actualSequence.toSet)

  }

  "Method getSessionId" should
  """return a Dataframe having columns as "user_id","sessionId","click_time","activity_time"
      |when a dataframe with columns "click_ts", "user_id" are passed""".stripMargin in {

    //given
    val testSparkSession = SparkSession
      .builder()
      .appName("testSparkSession")
      .config("spark.master", "local[1]")
      .getOrCreate()
    import testSparkSession.implicits._
    val rawDataframe = Seq(("2018-01-01 11:00:00", "u1"),
      ("2018-01-01 12:10:00", "u1"),
      ("2018-01-01 13:00:00", "u1"),
      ("2018-01-01 13:25:00", "u1"),
      ("2018-01-01 14:40:00", "u1"),
      ("2018-01-01 15:10:00", "u1"),
      ("2018-01-01 16:20:00", "u1"),
      ("2018-01-01 16:50:00", "u1"),
      ("2018-01-01 11:00:00", "u2"),
      ("2018-01-02 11:00:00", "u2")).toDF("click_ts", "user_id")

    //when
    val actualDataframe = SessionIdGenerator.getSessionIds(rawDataframe)
    val expectedDataframe = Seq(
      ("u1","c2938586ac2cf10843b5b337750f4b45","2018-01-01 11:00:00",0),
      ("u1","c60a5f434a9356346239d9bc701bdb52","2018-01-01 12:10:00",0),
      ("u1","c60a5f434a9356346239d9bc701bdb52","2018-01-01 13:00:00",3000),
      ("u1","c60a5f434a9356346239d9bc701bdb52","2018-01-01 13:25:00",1500),
      ("u1","ca897b641448ea9c8ce43fe2caf858d2","2018-01-01 14:40:00",0),
      ("u1","ca897b641448ea9c8ce43fe2caf858d2","2018-01-01 15:10:00",1800),
      ("u1","d457823da32b648321f881d9ae3b9d65","2018-01-01 16:20:00",0),
      ("u1","d457823da32b648321f881d9ae3b9d65","2018-01-01 16:50:00",1800),
      ("u2","9ed03b43a1092f43c4f30b399c1c4df5","2018-01-01 11:00:00",0),
      ("u2","6597e108764db28de7fa2e086640cc11","2018-01-02 11:00:00",0)
    ).toDF("user_id","sessionId","click_time","activity_time")

    //assert
    assertResult(true)(expectedDataframe.except(actualDataframe).count() == 0)

  }

}
