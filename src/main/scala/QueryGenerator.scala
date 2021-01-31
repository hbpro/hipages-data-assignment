package au.com.hipages

object QueryGenerator {
  def getAnswer2(tableName: String) =
    "SELECT time_bucket, url_level1, activity, " +
    "count(activity) as activity_count, " +
    "count(DISTINCT user_id) as user_count " +
    "FROM " + tableName + " " +
    "GROUP BY time_bucket, url_level1, activity"
}
