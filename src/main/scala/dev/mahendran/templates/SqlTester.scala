package dev.mahendran.templates

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SqlTester(@transient val sc: SparkContext) {
  def process( path: String) {
    val stringRdd = sc.textFile(path)
   stringRdd.map(x=> (x.split(",").apply(0), x)).groupByKey().map(x=>
     {
       (x._1, x._2.map { x => x.split(",").apply(8).toDouble}.reduceLeft(_ + _))
     }  
   
   ).foreach { x => println(x) }
    stringRdd.foreach { x => println("=====================" + x) }
  }
}

object SqlTester extends App{
  val path = args(0)
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  
  val spark  = SparkSession.builder().appName("Template").getOrCreate()
  
  import spark.implicits._
  
//  spark.sql("Select * from Table")
  
  println("==================" + path)
//  val path: String = ClassLoader.getSystemResource("sample_cc.csv").getPath
  new SqlTester(sc).process(path)
}