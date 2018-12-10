package dev.mahendran.templates

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.Row
import org.apache.spark.streaming.kafka010.HasOffsetRanges

object StreamingSQLApplication extends App {
  val sparkConf = new SparkConf()
  val sparkContext = new SparkContext(sparkConf)

  val ssc = new StreamingContext(sparkContext, Seconds(20))

  val spark = SparkSession
    .builder().config(sparkConf).appName("Mahendran-SQLStreaming-APP")
    .getOrCreate()


    //To convert RDD to DFs
  import spark.implicits._

  /**
   * Modify it late
   */
  spark.conf.set("spark.sql.shuffle.partitions", 6)
  spark.conf.set("spark.executor.memory", "2g")


  ssc.start()
  ssc.awaitTermination()
}
