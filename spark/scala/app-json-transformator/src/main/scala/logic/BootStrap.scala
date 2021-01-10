package logic

import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.rdd.RDD

object BootStrap extends App{

  val log = Logger.getLogger(BootStrap.getClass)
  PropertyConfigurator.configure("src/main/resources/conf/log4j.properties")

  //Creating spark session
  val spark = SparkSession
    .builder()
    .appName("app-ts-generate-sessionid")
    .config("spark.master", "local[1]")
    .getOrCreate()

  log.info("Spark session object created successfully.")

  val sc = spark.sparkContext

//val df = spark.read.option("multiline",true).json("src/main/resources/source/input.json")



  //First Approach :- Create a case class which extends Anonymus Function.This extends from Function1 trait.
  // It avoids "Task Not Serializable" issue.Why? Case Class by default extends Serializable
  // and hence the issue is avoided.
  //There are 2 apply methods in this context.First apply method is the case class apply method. For ex :- Val a = Anonym().here case class
  // companio object apply method is called which returns a case class object.Second apply method comes from Function 1 trait.So we have to
  // override this to acheive our fubnctionality.
  //Q) Why Function1 trait not Function 2 or anything else?
  //A) Map Partitions takes a function 1 as a parameter f : scala.Function1[scala.Iterator[T], scala.Iterator[U]]

  //Second Approach:- Make a function which accepts Iterator as a argument and return type is also Iterator.Here we may get serialization
  // issue.Need to prove this point by wirting some code.Will do it later

  //Third Approach:-Make a class which extends Serializable and Anonymus Function.This extends from Function1 trait.Unlike case class a normal
  // class doeensot extend Serializable.Here we need to first create object of this class by "new" keyword and then we can use the object of
  // it in map partitions. In this context we have only 1 apply method whh is of Function1 trait.




  val rdd = sc.wholeTextFiles("src/main/resources/source/").map(_._2)


val anonym = new Anonym()


 // rdd.mapPartitions( new some().anon(_)).foreach(println(_))

  rdd.mapPartitions(new Anonym()).foreach(println(_))

class some ()
  {
    val g = "work"
    def anon(x:Iterator[String]):Iterator[String]={
      x.map(x => g)
    }
  }



  /*def anon(x:Iterator[String]):Iterator[String]={


    x.map(x => new some())
  }*/


  //First Approach
  /*case class Anonym () extends (Iterator[String] => Iterator[String])
  {

    def apply(iterator:Iterator[String]): Iterator[String] = {
      {

        iterator.map(x => "WORKING" )
      }



    }
  }*/



  //Third Approach
  class Anonym () extends (Iterator[String] => Iterator[String]) with Serializable
  {

    def apply(iterator:Iterator[String]): Iterator[String] = {
    {

      iterator.map(x => "WORKING" )
    }



    }
  }






  //Execution starting point

}
