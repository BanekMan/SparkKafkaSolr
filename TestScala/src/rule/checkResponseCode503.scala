package rule

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object checkResponseCode503 {
  /**
   * Checking code 503 in column ResponseCode and print on console, IP and browsers for this sites.
   */
  def checkCode503(newDataFrame: DataFrame) {
    val checkCode = newDataFrame.filter(r => (r(5) == 503))
    checkCode.foreach(f => println("Code 503 was type for IP " + f(0) + " and userAgent " + f(7)))
  }
}