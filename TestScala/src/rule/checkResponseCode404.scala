package rule

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object checkResponseCode404 {
       /**
   * Checking code 404 in column ResponseCode and print on console, IP and browsers for this sites.
   */
    def checkCode404(newDataFrame: DataFrame){
         val checkCode = newDataFrame.filter(r => (r(5) ==404))
         checkCode.foreach(f => println("Code 404 was type for IP " + f(0) + " and userAgent " + f(7)))
    }
}