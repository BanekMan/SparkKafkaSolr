package main.scala

import com.redislabs.provider.redis._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.solr.common._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.kafka010.{ ConsumerStrategies, KafkaUtils, LocationStrategies }

import scala.collection.JavaConversions.asJavaCollection
import scala.collection.mutable.ArrayBuffer

object TestMain {

  val log = Logger.getLogger("TestMain")

  def main(args: Array[String]): Unit = {

    log.debug("Entering application.")
    log.info("Start Application TestMain.")

    //  Mocking Hadoop claster

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    //  Create Spark Context values

    val conf = new SparkConf()
      .setAppName("LearningSpark")
      .setMaster("local")
      .set("spark.driver.allowMultipleContexts", "true")
      //      redis Ports
      .set("redis.host", "127.0.0.1")
      .set("redis.port", "6379")
    //      .set("redis.auth", "a43456bc25")
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
      ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    // Map and write stream to records in files

    val data = messages.map(record => record.value())
    val splitData = data.map(_.split("""\|\|"""))

    // Write DStrem to text document
    //    data.foreachRDD(p => p.toDF().write.mode("append").text("C:\\kafka_2.12-1.0.0\\Zapis Z Kafki przez Spark"))

    data.foreachRDD(p => p.saveAsTextFile("C:\\kafka_2.12-1.0.0\\Zapis Z Kafki przez Spark\\save"))

    //  Creating Solr Document
    def getSolrDocument(id: String, date: String, requestType: String, requestPage: String, httpProtocolVersion: String, responseCode: Int, responseSize: Int, userAgent: String): SolrInputDocument = {
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
      val solrDocsRDD = df.rdd.map { x =>
        getSolrDocument(
          x.getAs[String]("id"),
          x.getAs[String]("date"),
          x.getAs[String]("requestType"),
          x.getAs[String]("requestPage"),
          x.getAs[String]("httpProtocolVersion"),
          x.getAs[Int]("responseCode"),
          x.getAs[Int]("responseSize"),
          x.getAs[String]("userAgent"))
      }
      solrDocsRDD.foreachPartition { partition =>
        {
          val batch = new ArrayBuffer[SolrInputDocument]()
          println("Writing odcuments to SOLR")
          while (partition.hasNext) {
            batch += partition.next()
            WriteDataSolr.client.add(asJavaCollection(batch))
          }
        }
      }
      WriteDataSolr.client.commit
      println("Documents have been saved to SOLR.")
    }

    //    Run WriteToCache foreachRDD

    splitData.foreachRDD({ row =>
      if (!row.isEmpty()) {
        val createRDD = row.map { x => (x(0), x(1), x(2), x(3), x(4), x(5).toInt, x(6).toInt, x(7)) }
        //         Read from Redis zwraca RDD
        def readFromRedis(dataRDD: RDD[(String, String, String, String, String, Int, Int, String)]): RDD[(String, String)] = {
          val modRDD = dataRDD.map(x => (x._1))
          val modArray = modRDD.collect().distinct
          val oldDataRDD = sc.fromRedisKV(modArray)
          oldDataRDD
        }
        //  Date to CompareRule
        val oldDataRDDr = readFromRedis(createRDD)
        val newDataRDD = createRDD.map(x => (x._1, (x._2, x._3, x._4, x._5, x._6.toString(), x._7.toString(), x._8).toString()))
        val emptyDataRDD = sc.parallelize(List(("", "")))
        //  Run CompareRule
        val backDataFromRule = RuleEngine.runCompareRule(newDataRDD, oldDataRDDr, emptyDataRDD)        
        val backDataFromRule2 = RuleEngine.runLogRepetitions(backDataFromRule, oldDataRDDr, emptyDataRDD)
        
        println("Dane po uruchomienu reguly: ")
        backDataFromRule2.toDF().show(false)
        //       Write Data to Redis
        def writeToRedis(dataRDD: RDD[(String, String)]) {
          sc.toRedisKV(dataRDD)
        }
        //        Zapis do Redisa nowych danych
        writeToRedis(backDataFromRule2)

        val dataFrame = createRDD.toDF("id", "date", "requestType", "requestPage", "httpProtocolVersion", "responseCode", "responseSize", "userAgent")
//        RuleEngine.runRules(dataFrame)
        val writeSolr = writeToCache(dataFrame)
      }
    })

    //    Start the computation
    println("Start application")
    ssc.start()
    println("StreamingContext started.")
    ssc.awaitTermination()
  }
}