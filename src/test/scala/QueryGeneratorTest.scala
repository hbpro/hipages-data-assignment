package au.com.hipages

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class QueryGeneratorTest extends AnyFlatSpec{

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

  "Answer2DF number of columns and rows " should "be 5 and 4" in {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val f = fixture
    f.explodedDF.createOrReplaceTempView("exploded_table")
    val answer2 = f.spark.sql(QueryGenerator.getAnswer2("exploded_table"))

    assert(answer2.count === 2)
    assert(answer2.columns.length === 5)
  }

}
