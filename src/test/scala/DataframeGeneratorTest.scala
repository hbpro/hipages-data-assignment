package au.com.hipages

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class DataframeGeneratorTest extends AnyFlatSpec{

  def fixture =
    new {
      val jsonStrSeq = Seq(
        """{ "event_id" : "893479324983546", "user" : { "session_id" : "564561", "id" : 56456 , "ip" : "111.222.333.4" }, "action" : "page_view", "url" : "https://www.hipages.com.au/articles", "timestamp" : "02/02/2017 20:22:00"}""",
        """{ "event_id" : "349824093287032", "user" : { "session_id" : "564562", "id" : 56456 , "ip" : "111.222.333.5" }, "action" : "page_view", "url" : "https://www.hipages.com.au/connect/sfelectrics/service/190625", "timestamp" : "02/02/2017 20:23:00"}""",
        """{ "event_id" : "324872349721534", "user" : { "session_id" : "564563", "id" : 56456 , "ip" : "111.222.33.66" }, "action" : "page_view", "url" : "https://www.hipages.com.au/get_quotes_simple?search_str=sfdg", "timestamp" : "02/02/2017 20:26:00"}""",
        """{ "event_id" : "213403460836344", "user" : { "session_id" : "564564", "id" : 56456 , "ip" : "111.222.3.77" }, "action" : "button_click", "url" : "https://www.hipages.com.au/advertise", "timestamp" : "01/03/2017 20:21:00"}""",
        """{ "event_id" : "235487204723104", "user" : { "session_id" : "564565", "id" : 56456 , "ip" : "111.22.22.4" }, "action" : "page_view", "url" : "https://www.hipages.com.au/photos/bathrooms", "timestamp" : "02/02/2017 20:12:34"}"""
      )
      val spark = SparkSession
        .builder()
        .appName("test")
        .master("local[*]")
        .getOrCreate()

      import spark.implicits._
      val baseDF = spark.read.json(jsonStrSeq.toDS)
      val explodedDF = DataframeGenerator.intermediateDF(baseDF)
    }

  "Number of columns and rows in baseDF " should "be 5" in {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val f = fixture
    assert(f.baseDF.count === 5)
    assert(f.baseDF.columns.length === 5)
  }

  "Number of columns in explodedDF " should "be 15" in {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val f = fixture
    assert(f.explodedDF.columns.length === 15)
  }

  "Answer1DF" should "contain 6 columns consisting user_id, time_stamp, " +
    "url_level1, url_level2, url_level3 and activity " in {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val f = fixture
    val answer1DF = DataframeGenerator.answer1DF(f.explodedDF)
    assert(answer1DF.columns.length === 6)
    assert(answer1DF.columns.contains("user_id"))
    assert(answer1DF.columns.contains("time_stamp"))
    assert(answer1DF.columns.contains("url_level1"))
    assert(answer1DF.columns.contains("url_level2"))
    assert(answer1DF.columns.contains("url_level3"))
    assert(answer1DF.columns.contains("activity"))
  }

}
