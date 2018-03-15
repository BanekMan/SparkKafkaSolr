package rule

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import main.scala.TestMain

/**
 * Method to compare ResponseCode in new and old data
 * Where value are equal new data is empty
 */

object compareResponseCode {
  def areEqual(newData: RDD[(String, String)], oldData: RDD[(String, String)], emptyData: RDD[(String, String)]): RDD[(String, String)] = {
    val newDataOne = newData.map(f => f._1).collect()
    val oldDataOne = oldData.map(f => f._1).collect()
    if (newDataOne.deep == oldDataOne.deep) {
      val newDataSecondSplit = newData.map(f => f._2).map(_.split(",")).collect()
      val oldDataSecondSplit = oldData.map(f => f._2).map(_.split(",")).collect()
      val newData5column = newDataSecondSplit.map(f => f(4))
      val oldData5column = oldDataSecondSplit.map(f => f(4))
      if (newData5column.deep == oldData5column.deep) {
        val zwrotka = "rowne ID i dane"
        emptyData
      } else newData
    } else newData
  }
}