package rule

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object countRepetitionId {
       /**
   * Counting repetitions of Id-s. Show the number of repeats of the Id column.
   */
  def countSameId(newDataFrame: DataFrame){
    val singleCol = newDataFrame.select("id") 
    println("Calculations of repetitions for column ID: ")
    val countRepetition = singleCol.groupBy("id").agg(count("id").as("Repetitions")).orderBy(desc("Repetitions"))
    println(countRepetition.count()) 
    countRepetition.show(countRepetition.count().toInt, false)
    
  }
}