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
//import scala.collection.parallel.ParIterableLike.Fold

object TestMain {

  // Create logger for console view
  val log = Logger.getLogger("TestMain")

  //  ====================================
  //  MAIN METHOD
  //  ====================================

  def main(args: Array[String]): Unit = {

    log.debug("Entering application.")
    log.info("Start Application TestMain.")

    //  Mocking Hadoop claster

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    //  Create Spark Config for Spark Context values

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

    // Create direct Kafka stream with brokers and topics

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    // Map and split data

    val data = messages.map(record => record.value())
    val splitData = data.map(_.split("""\|\|"""))

    // Write DStrem to text document
    data.foreachRDD(p => p.saveAsTextFile("C:\\kafka_2.12-1.0.0\\Zapis Z Kafki przez Spark\\save"))

    //  Schema Solr document
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

    //  Write DataFrame to SolrDocumentSchema and add it to Solr
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
          println("Writing documents to SOLR")
          while (partition.hasNext) {
            batch += partition.next()
            WriteDataSolr.client.add(asJavaCollection(batch))
          }
        }
      }
      WriteDataSolr.client.commit
      println("Documents have been saved to SOLR.")
    }
    //   =================

    //   =================
    //    Run WriteToCache foreachRDD

    splitData.foreachRDD({ row =>
      if (!row.isEmpty()) {
        val createRDD = row.map { x => (x(0), x(1), x(2), x(3), x(4), x(5).toInt, x(6).toInt, x(7)) }
        //  Read data from Redis
        def readFromRedis(dataRDD: RDD[(String, String, String, String, String, Int, Int, String)]): RDD[(String, String)] = {
          val modRDD = dataRDD.map(x => (x._1))
          val modArray = modRDD.collect().distinct
          val oldDataRDD = sc.fromRedisKV(modArray)
          oldDataRDD
        }

            //  Data for CompareRule
            val oldDataRDDr = readFromRedis(createRDD).sortBy(f => f._1, true)
            val newDataRDD = createRDD.map(x => (x._1, (x._2, x._3, x._4, x._5, x._6.toString(), x._7.toString(), x._8).toString()))
            val emptyDataRDD = sc.parallelize(List(("", "")))
            //  Run CompareRules
            def writeToRedis(dataRDD: RDD[(String, String)]) {
              sc.toRedisKV(dataRDD)
            }
            val backDataFromRule = RuleEngine.runCompareRule(newDataRDD, oldDataRDDr, emptyDataRDD)
            //   Write new Data from Rule 1 to Redis
            val backDataFromRule2 = RuleEngine.runCompareResponseCode(backDataFromRule, oldDataRDDr, emptyDataRDD)
            //   Write new Data from Rule 2 to Redis
            val backDataFromRule3 = RuleEngine.runCompareResponseSize(backDataFromRule2, oldDataRDDr)
            //   Write new Data from Rule 3 to Redis
            writeToRedis(backDataFromRule3)

            val dataWriteToSolr = backDataFromRule3.map(f => (f._1, f._2.split(","))).map(f => (f._1, f._2(0), f._2(1), f._2(2), f._2(3), f._2(4), f._2(5), f._2(6)))
            val dataWriteToSolrInt = dataWriteToSolr.map(f => (f._1, f._2, f._3, f._4, f._5, f._6.toInt, f._7.toInt, f._8))
            val dataFrameToSolrDoc = dataWriteToSolrInt.toDF("id", "date", "requestType", "requestPage", "httpProtocolVersion", "responseCode", "responseSize", "userAgent")
            RuleEngine.runRules(dataFrameToSolrDoc)
            if (!backDataFromRule3.isEmpty() || backDataFromRule3 == oldDataRDDr) {
              val writeSolr = writeToCache(dataFrameToSolrDoc)
            }
      }
    })

    //    Start the computation
    println("Start application")
    ssc.start()
    println("StreamingContext started.")
    ssc.awaitTermination()
  }
}