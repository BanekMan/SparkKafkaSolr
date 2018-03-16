package rule

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import main.scala.TestMain

/**
 * Method that compare Id from new and old data
 * For the same Id compare ResponseCode in second String.
 * When ResponseCode is same method check if ResponseCode is the same
 */

object compareResponseCode {
  def areEqual(newData: RDD[(String, String)], oldData: RDD[(String, String)], emptyData: RDD[(String, String)]): RDD[(String, String)] = {
    val newDataOne = newData.map(f => f._1).collect()
    val oldDataOne = oldData.map(f => f._1).collect()
    if (newDataOne.deep == oldDataOne.deep) {
      val newResponseCode = newData.map(f => f._2).map(_.split(",")).collect().map(f => f(4))
      val oldResponseCode = oldData.map(f => f._2).map(_.split(",")).collect().map(f => f(4))
      if (newResponseCode.deep == oldResponseCode.deep) {
        val newResponseSize = newData.map(f => f._2).map(_.split(",")).collect().map(f => f(5))
        val oldResponseSize = oldData.map(f => f._2).map(_.split(",")).collect().map(f => f(5))
        if (newResponseSize(0).toInt == oldResponseSize(0).toInt){
          emptyData
        } else newData
      } else newData
    } else newData
  }
}