package rule

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object OverMaxResponseSize {
       /**
   * Check max size for column Response Size and value 200 in responseCode.
   */
      def checkMaxResponseSize200(newDataFrame: DataFrame){
         val filterCode = newDataFrame.filter(r => (r(5) =="  200 "))
         val countMax = filterCode.agg(first("responseSize"), max("responseSize"))  
         countMax.printSchema()
         println(countMax)
//         val countMax = filterCode.filter(col("responseSize") >= 2000)
//     countMax.foreach(f => println("Code 503 was type for IP " + f(0) + " and userAgent " + f(6)))
  }
}