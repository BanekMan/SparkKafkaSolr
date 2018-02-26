package main.scala

import rule._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object RuleEngine {
/**
 * Runinng all rules in package "Rule", for dataframe from Kafka stream.
 */
  def runRules (df: DataFrame){
    //  Count code 404 i column responseCode
    countResponseCode404.countCode404(df)
    //  Count code 503 i column responseCode
    countResponseCode503.countCode503(df)
    //  Check code 503 responseCode 
    checkResponseCode503.checkCode503(df) 
    //  Check code 503 responseCode 
    checkResponseCode404.checkCode404(df)
//    Show max value for reponseSize
    OverMaxResponseSize.checkMaxResponseSize200(df)
  }
  

}