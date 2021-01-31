package au.com.hipages

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, desc, max, min, timestamp_seconds}

object Driver {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("hipages")
      .master("local[*]")
      .getOrCreate()

    val baseDF = spark
      .read
      .schema(StructObject.getStruct)
      .option("mode", "DROPMALFORMED")
      .json(SystemParams.getBasePath + "/source_event_data.json")

    val explodedDF = DataframeGenerator.intermediateDF(baseDF)

    val answer1 = DataframeGenerator.answer1DF(explodedDF)

    answer1
      .coalesce(1)
      .write.format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("path", SystemParams.getOutputPath + "/answer1.csv")
      .save()

    explodedDF.createOrReplaceTempView("exploded_table")

    val answer2 = spark.sql(QueryGenerator.getAnswer2("exploded_table"))

    answer2
      .coalesce(1)
      .write.format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .option("path", SystemParams.getOutputPath + "/answer2.csv")
      .save()

    val sessionLengthDF = DataframeGenerator.sessionLengthDF(explodedDF)

    sessionLengthDF
      .coalesce(1)
      .write.format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("path", SystemParams.getOutputPath + "/sessionLength.csv")
      .save()
  }
}
