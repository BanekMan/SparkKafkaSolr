package main.scala


import com.redislabs.provider.redis._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object RedisConnect {
  def writeToRedis(dataRDD: RDD[(String, String, String, String, String, Int, Int, String)]){
//  val modRDD = dataRDD.map(x => (x._1, (x._2, x._3, x._4 , x._5, x._6.toString(), x._7.toString(), x._8).toString()))
//  sc.toRedisKV(modRDD)
  }
}