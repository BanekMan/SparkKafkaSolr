package rule

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object MaxResponseSize200 {
       /**
   * Check max size for column Response Size and value 200 in responseCode.
   */
      def checkMaxResponseSize200(newDataFrame: DataFrame){
         val filterCode = newDataFrame.filter(r => (r(5) ==200))
//         filterCode.show()
         
         val getRowWithMax = filterCode.select("id", "responseSize")
         val maxRow = getRowWithMax.orderBy(desc("responseSize")).first()
         println("Max respose size for response Code 200 is: " + maxRow.get(1) + " and IP is " + maxRow.get(0))
                
//         val countMax = filterCode.agg( max("responseSize")).head()
//         val colMax = countMax.getInt(0)
//         println("Max respose size for response Code 200 is: " + colMax.toString())

  }
}