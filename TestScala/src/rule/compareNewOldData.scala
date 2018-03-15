package rule
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import main.scala.TestMain

/**
 * Method that compare Id from new and old data
 * For the same Id compare second string.
 * When Data is same method return empty RDD 
 */

object compareNewOldData {
  def compareRDD (newData: RDD[(String, String)], oldData: RDD[(String, String)], emptyData: RDD[(String, String)]): RDD[(String, String)] = {
    val newDataOne = newData.map(f=> f._1)
    val oldDataOne = oldData.map(f=> f._1)
    if (newDataOne == oldDataOne){
      val newDataSecond = newData.map(f=> f._2)
      val oldDataSecond = oldData.map(f=> f._2)
      if (newDataSecond == oldDataSecond){
        emptyData
      } else newData
    } else newData    
  }
}