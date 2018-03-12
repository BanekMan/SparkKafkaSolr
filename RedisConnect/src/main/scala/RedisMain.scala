package main.scala

import com.redislabs.provider.redis._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

class RedisMain {

}
object RedisMain {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf()
      .setAppName("RedisMain")
      .setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("redis.host", "localhost")
      .set("redis.port", "6379")
    //          .set("redis.auth", "a43456bc25")
    //    val ssc = new StreamingContext(conf, Seconds(20))
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    //    //    val redisServerDnsAddress = "localhost"
    //    //    val redisPortNumber = 6379
//    val redisConfigStandalone = new RedisConfig(new RedisEndpoint("localhost", 6379))
//    //    val stringRDD = sc.parallelize(Seq(("StringC", "StringD"), ("String3", "String4")))
//    //    //    sc.toRedisKV(sc.parallelize(("key1", "val1") :: Nil))
//    //    sc.toRedisKV(stringRDD)(redisConfigStandalone)
//
//    //    sc.getConf.setMaster("local").setAppName("RedisMain").set("redis.port", "6379").set("redis.host", "localhost")
//    sc.toRedisKV(sc.parallelize(("testing", "3") :: Nil), (6379))(redisConfigStandalone)
//    val valuesRDD = sc.fromRedisKV("testing", (6379))(redisConfigStandalone)
//    valuesRDD.collect


    val redisServerDnsAddress = "REDIS_HOSTNAME"
    val redisPortNumber = 6379
    val stringRDD = sc.parallelize(Seq(("StringC", "StringD"), ("String3", "String4")))
    sc.toRedisKV(stringRDD, (6379))
  }
}
