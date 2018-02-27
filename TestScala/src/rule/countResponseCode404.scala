package rule

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

//   Count 404 in responseCode
object countResponseCode404 {
  
   /**
   * Counting all rows with code 404 in column ResponseCode and print in console
   * number of occurrences.
   */
  def countCode404(newDataFrame: DataFrame){
          val counterCode = newDataFrame.filter(r => (r(5) ==404))
         .count()
     println("Number of occurrences for code 404 = " + counterCode.toString())
  }
}