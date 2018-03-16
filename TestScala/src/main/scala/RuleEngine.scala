package main.scala

import rule._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

object RuleEngine {
  /**
   * Running all rules in package "Rule", for dataframe from Kafka stream.
   */
  def runRules(df: DataFrame) {
    countResponseCode404.countCode404(df)
    countResponseCode503.countCode503(df)
    checkResponseCode503.checkCode503(df)
    checkResponseCode404.checkCode404(df)
    MaxResponseSize200.checkMaxResponseSize200(df)
    countRepetitionId.countSameId(df)
  }
  
  /**
   * Running rule to compare all Strings from DataLake
   */
  def runCompareRule(newData: RDD[(String, String)], oldData: RDD[(String, String)], emptyData: RDD[(String, String)]): RDD[(String, String)] = {
    compareNewOldData.compareRDD(newData, oldData, emptyData)
  }
  /**
   * Running rule to compare ResponseCode
   */
  def runCompareResponseCode(newData: RDD[(String, String)], oldData: RDD[(String, String)], emptyData: RDD[(String, String)]): RDD[(String, String)]={
    compareResponseCode.areEqual(newData, oldData, emptyData)
  }
  /**
   * Running rule to compare ResponseSize
   */
  def runCompareResponseSize(newData: RDD[(String, String)], oldData: RDD[(String, String)]): RDD[(String, String)]={
    compareResponseSize.sizeAreEqual(newData, oldData)
  }
}