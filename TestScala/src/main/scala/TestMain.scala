package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
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
    val topics = Array("test", "test3", "test5")
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
    data.foreachRDD(x =>print(x))
    val splitData = data.flatMap(_.split(" "))
    splitData.foreachRDD(p => p.toDF().write.mode("append").text("C:\\kafka_2.12-1.0.0\\Zapis Z Kafki przez Spark"))
    splitData.foreachRDD(p => p.saveAsTextFile("C:\\kafka_2.12-1.0.0\\Zapis Z Kafki przez Spark\\save"))

//  Write data to Solr
    
    val doc = new SolrInputDocument()
    def getSolrDocument(): SolrInputDocument = {
      val document = new SolrInputDocument()
      document.addField("id", "8")
      document.addField("name", "imie8")
      document.addField("age", "38")
      document.addField("addr", "Adres8")
      document
    }
    val firstClient = WriteDataSolr.client.add(getSolrDocument)
    WriteDataSolr.client.commit
    
    
// Start the computation   
    ssc.start()
    ssc.awaitTermination()
  }
}