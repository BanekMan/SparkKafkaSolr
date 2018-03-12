package main.scala

import rule._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

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
}