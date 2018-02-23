package main.scala

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.solr.common.SolrInputDocument


class ReadData {
  
}

object ReadData{
   def main(args: Array[String]): Unit = {
     
     
//  Mocking Hadoop claster
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
    
 val spark = SparkSession
  .builder
  .appName("StructuredNetworkWordCount")
  .getOrCreate()
  
import spark.implicits._
    

//val lines = spark.readStream
//  .format("socket")
//  .option("host", "localhost")
//  .option("port", 9092)
//  .load()
  
  
  val df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "test1")
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .as[(String, String)]
  
 def getSolrDocument(host: String, page: String): SolrInputDocument = {
      val document = new SolrInputDocument()
      document.addField("host", host)
      document.addField("page", page)
      document
    }
    
val solrDocsRDD = df.rdd.map{
      case Row(host: String, page: String) => getSolrDocument(host, page)
    }
    
    WriteDataSolr.client.add(getSolrDocument)
    WriteDataSolr.client.commit
  }
}