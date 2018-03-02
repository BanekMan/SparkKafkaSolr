package rule

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

//   Count code 503 in responseCode
object countResponseCode503 {
  /**
   * Counting all rows with code 503 in column ResponseCode and print in console
   * number of occurrences.
   */
  def countCode503(newDataFrame: DataFrame) {
    val counterCode = newDataFrame.filter(r => (r(5) == 503))
      .count()
    println("Number of occurrences for code 503 = " + counterCode.toString())
  }
}