package au.com.hipages

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object StructObject {
  def getStruct()={
    val innerStruct =
      StructType(
        StructField("session_id", StringType, false) ::
          StructField("id", StringType, false) ::
          StructField("ip", StringType, false) :: Nil
      )

    val struct = StructType(
      StructField("event_id", StringType, false) ::
        StructField("user", innerStruct, false) ::
        StructField("action", StringType, false) ::
        StructField("url", StringType, false) ::
        StructField("timestamp", StringType, false) ::Nil
    )

    struct
  }
}
