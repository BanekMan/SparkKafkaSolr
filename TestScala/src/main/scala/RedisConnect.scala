package main.scala


import com.redislabs.provider.redis._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object RedisConnect {
  def writeToRedis(dataRDD: RDD[(String, String, String, String, String, Int, Int, String)]){

//  val sc2 = new SparkContext(new SparkConf()
//    .setMaster("local")
//    .setAppName("TestScala")
//    // initial redis host - can be any node in cluster mode
//    .set("redis.host", "localhost")
//    // initial redis port
//    .set("redis.port", "6379")
//    .set("spark.driver.allowMultipleContexts", "true"))
//
//  //  Special keys RDD
//  val keysRDD2 = sc2.fromRedisKeys(Array("id", "rest"), 3)
//  val modRDD = dataRDD.map(x => (x._1, (x._2, x._3, x._4 , x._5, x._6.toString(), x._7.toString(), x._8).toString()))
//  sc2.toRedisKV(modRDD)
  }
}