package rule

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import main.scala.TestMain

object compareResponseSize {
  
  def sizeAreEqual (newData: RDD[(String, String)], oldData: RDD[(String, String)]): RDD[(String, String)] = {
    val newDataOne = newData.map(f => f._1).collect()
    val oldDataOne = oldData.map(f => f._1).collect()
    if (newDataOne.deep == oldDataOne.deep) {
      val newDataSecondSplit = newData.map(f => f._2).map(_.split(",")).collect().map(f => f(5))
      println("nowe kolumna Size do porownania " ++ newDataSecondSplit)
      val oldDataSecondSplit = oldData.map(f => f._2).map(_.split(",")).collect().map(f => f(5))
      println("stara kolumna Size do porownania " ++ oldDataSecondSplit)
      if (oldDataSecondSplit(0).toInt >= newDataSecondSplit(0).toInt  ) {
        oldData
      } else newData
    } else newData
  } 
}