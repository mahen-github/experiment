package dev.mahendran.templates

import java.util.Date

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class HdfsWriter(@transient val sc: SparkContext) {

  def writeTextFile(rdd: RDD[(String, Population)], path:String)=  {
    rdd.saveAsTextFile(path)
    rdd
  }

  def writeTextFileByType(rdd: RDD[(String, Population)], path:String)= {

    val subDirName =  System.currentTimeMillis.toString
    val jobConf = new JobConf(rdd.context.hadoopConfiguration)

      jobConf.set("mapreduce.output.basename", subDirName)

    rdd.saveAsHadoopFile(
                    path,
                    classOf[String],
                    classOf[Population],
                    classOf[MultipleOutputFormatWriter],jobConf
                    )
    rdd
  }

  def writeSequenceFiles(rdd: RDD[(Text, Text)], path:String): Unit = {
     rdd.saveAsSequenceFile(path + "seq")
  }

  def writeSequenceFilesByEntity(rdd: RDD[(Text, Text)], path:String): Unit = {
    val str = "EntityName"

    val jobConf = new JobConf(rdd.context.hadoopConfiguration)
    if (str.length >= 1) {
      jobConf.set("mapreduce.output.basename", str)
    }
    else {
      jobConf.set("mapreduce.output.basename", "TestingMultiOutput")
    }

    rdd.saveAsHadoopFile(
      path,
      classOf[Text],
      classOf[Text],
      classOf[MultipleOutputFormatWriter]
      )


    def writeTuples(value: (Text, Text), path: String): Unit = {
      val rdd = sc.parallelize(List((new Text("test"), new Text("123"))))

      rdd.saveAsSequenceFile(path)
    }
  }

}

object HdfsWriter {
  def configure(sc: SparkContext): HdfsWriter = {
    new HdfsWriter(sc)
  }
}
