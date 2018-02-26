package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.PreferConsistent
import org.apache.spark.streaming.kafka010.Subscribe
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.DirectKafkaInputDStream
import org.apache.kafka.common.serialization.StringDeserializer
import scala.collection.mutable.ArrayBuffer
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.solr.common._
import org.apache.spark.sql.{DataFrame, Row}
import scala.collection.JavaConversions.asJavaCollection
import org.apache.spark.streaming.dstream.DStream


object TestMain{
  def main(args: Array[String]): Unit = {
    
//  Mocking Hadoop claster
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")   
    
    //  Create Streaming values
    
    val conf = new SparkConf()
      .setAppName("Learning Spark")
      .setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")
    val ssc = new StreamingContext(conf, Seconds(20))
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    import sqlContext.implicits._
    
//  Create Kafka parameters for connection for Kafka broker version 0.10.0 or higher
    
    val brokers = "localhost:9092"
    val topics = Array("test1")
    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> brokers,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "use_a_separate_group_id_for_each_stream",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean))
        
// Create direct kafka stream with brokers and topics
        
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

// Map and write stream to records in files  
    val data =  messages.map(record => record.value()) 
//    data.foreachRDD(x =>print(x))
    val splitData = data.map(_.split("""\|\|"""))
    
// Write DStrem to text document
    data.foreachRDD(p => p.toDF().write.mode("append").text("C:\\kafka_2.12-1.0.0\\Zapis Z Kafki przez Spark"))
    data.foreachRDD(p => p.saveAsTextFile("C:\\kafka_2.12-1.0.0\\Zapis Z Kafki przez Spark\\save"))

   
//  Creating Solr Document 
    def getSolrDocument(id: String, date: String, requestType: String, requestPage: String, httpProtocolVersion: String, responseCode: String, responseSize: String, userAgent: String): SolrInputDocument = {
      val document = new SolrInputDocument()
      document.addField("id", id)
      document.addField("date", date)
      document.addField("requestType", requestType)
      document.addField("requestPage", requestPage)
      document.addField("httpProtocolVersion", httpProtocolVersion)
      document.addField("responseCode", responseCode)
      document.addField("responseSize", responseSize)
      document.addField("userAgent", userAgent)
      document
    }
  
//  Write DataFrame to SolrDocument and add to Solr
    def writeToCache(df: DataFrame): Unit = {
      val solrDocsRDD= df.rdd.map { x=> getSolrDocument(x.getAs[String]("id"),x.getAs[String]("date"), x.getAs[String]("requestType"), x.getAs[String]("requestPage"), x.getAs[String]("httpProtocolVersion"), x.getAs[String]("responseCode"), x.getAs[String]("responseSize"), x.getAs[String]("userAgent"))}
      solrDocsRDD.foreachPartition{ partition => {       
        val batch = new ArrayBuffer[SolrInputDocument]()
        while(partition.hasNext){
        batch += partition.next()
        WriteDataSolr.client.add(asJavaCollection(batch))  
        }
        }             
      } 
      WriteDataSolr.client.commit 
    }  
    
 //    Run WriteToCache foreachRDD
       splitData.foreachRDD({row=>
          val dF4 = row.map{x=> (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7))}
          val dF5 = dF4.toDF("id", "date", "requestType", "requestPage", "httpProtocolVersion", "responseCode", "responseSize", "userAgent")
         dF5.show(false)
         dF5.printSchema()
          RuleEngine.runRules(dF5)
          val writeSolr =  writeToCache(dF5)})         
          
//    Start the computation    
        ssc.start()
//    Await
        ssc.awaitTermination()
  }
}