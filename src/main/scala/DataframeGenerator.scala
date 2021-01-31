package au.com.hipages

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, column, concat, dayofmonth, hour, max, min, month, split, to_timestamp, to_utc_timestamp, year}
import org.apache.spark.sql.types.LongType

object DataframeGenerator {

  def intermediateDF(baseDF: DataFrame) = baseDF.select(
    col("*"),
    col("user.id") as "user_id",
    col("user.session_id"),
    col("user.ip"),
    col("action") as "activity")
    .withColumn("url_level1", split(col("url"), "/")(2))
    .withColumn("url_level2", split(col("url"), "/")(3))
    .withColumn("url_level3", split(col("url"), "/")(4))
    .withColumn("datetype_timestamp",
      to_timestamp(col("timestamp"), "MM/dd/yyyy HH:mm:ss"))
    .withColumn("UTC",to_utc_timestamp(col("datetype_timestamp"),"Australia/Sydney"))
    .withColumn("time_bucket", concat(year(col("UTC")),
      month(col("UTC")), dayofmonth(col("UTC")),
      hour(col("UTC"))
    ))

  def answer1DF(explodedDF: DataFrame) = explodedDF.select(
    col("user_id"),
    col("UTC") as "time_stamp",
    col("url_level1"),
    col("url_level2"),
    col("url_level3"),
    col("activity")
  )

  def sessionLengthDF(explodedDF: DataFrame) = {
    val userSessionWindow = Window
      .partitionBy("user_id", "session_id")

    val fMin = min(col("datetype_timestamp")).over(userSessionWindow)
    val fMax = max(col("datetype_timestamp")).over(userSessionWindow)

    val intermediateDF = explodedDF
      .withColumn("start_time", fMin)
      .withColumn("end_time", fMax)

    val sessionLengthDF = intermediateDF
      .select(col("user_id"), col("session_id"),
        col("start_time"), col("end_time"))

    sessionLengthDF
  }

}
