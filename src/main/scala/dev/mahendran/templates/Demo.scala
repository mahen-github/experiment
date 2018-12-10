package dev.mahendran.templates

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

case class data(customer_id: Long, acct_num: Long, customer_profile: String,
                trans_num: Long, trans_date: String, trans_time: String,
                unix_time: Long, category: String, amt: Double, is_fraud: Short)

case class employee(
emp_no: Int,
birth_date: String,
first_name:String,
last_name:String,
gender: String,
hire_date: String
)

case class salary(
emp_no: Int,
salary:Int,
from_date: String,
to_date: String
)

object Demo extends App {

  val sparkConf = new SparkConf()
  val sc = new SparkContext(sparkConf)
  val spark = SparkSession
     .builder().config(sparkConf).appName("Mahendran-SQLStreaming-APP")
     .getOrCreate()

  /**
    * Modify it late
    */
  spark.conf.set("spark.sql.shuffle.partitions", 6)
  spark.conf.set("spark.executor.memory", "2g")

  val hadoopConf = sc.hadoopConfiguration
  val fs = FileSystem.get(hadoopConf)
  if(fs.exists(new Path(Constants.output_ROOT))){
    fs.delete(new Path(Constants.output_ROOT), true)
  }

  val lines = sc.textFile("/tmp/data/salaries.csv")
  val employeeData = sc.textFile("/tmp/data/employees_data.csv")
  val salaryData = sc.textFile("/tmp/data/salaries.csv")

  val transData = lines.map{
    line =>
      val tokens = line.split(",")
      data(tokens(0).toLong, tokens(1).toLong, tokens(2),
        tokens(3).toLong, tokens(4), tokens(5), tokens(6).toLong,
        tokens(7), tokens(8).toDouble, tokens(9).toShort)
  }

  //To convert RDD to DFs
  import spark.implicits._
  val employeeTable = employeeData.map{
    line =>
        val tokens = line.split(",")
        employee(tokens(0).toInt, tokens(1), tokens(2), tokens(3), tokens(4), tokens(5))
  }.toDF()

  val salaryTable = salaryData.map{
    line =>
      val tokens = line.split(",")
      salary(tokens(0).toInt, tokens(1).toInt, tokens(2), tokens(3))
  }.toDF()

//  val empWithSal = employeeTable.join(salaryTable)
//  empWithSal.collect().foreach{
//    rec => println(rec)
//  }

  sc.stop()
}
