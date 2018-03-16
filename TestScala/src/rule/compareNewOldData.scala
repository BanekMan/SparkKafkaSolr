package rule
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import main.scala.TestMain

/**
 * Method that compare Id from new and old data
 * For the same Id compare all second string.
 * When Data is same method return empty RDD 
 */

object compareNewOldData {
  def compareRDD (newData: RDD[(String, String)], oldData: RDD[(String, String)], emptyData: RDD[(String, String)]): RDD[(String, String)] = {
    val newDataOne = newData.map(f=> f._1).collect()
    val oldDataOne = oldData.map(f=> f._1).collect()
    if (newDataOne.deep == oldDataOne.deep){
      val newDataSecond = newData.map(f=> f._2).collect()
      val oldDataSecond = oldData.map(f=> f._2).collect()
      if (newDataSecond.deep == oldDataSecond.deep){
        emptyData
      } else newData
    } else newData    
  }
}