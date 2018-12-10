package dev.mahendran.templates

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App{

   val sparkConf = new SparkConf()
   val sc = new SparkContext(sparkConf)

   val hadoopConf = sc.hadoopConfiguration
   val fs = FileSystem.get(hadoopConf)
   if(fs.exists(new Path(Constants.training_output_ROOT))){
      fs.delete(new Path(Constants.training_output_ROOT), true)
   }

   val data = sc.textFile(Constants.trainingDataPath)

   println("================== getNumPartitions" + data.getNumPartitions)
   val cachedData = data.cache()
   val result = cachedData
                   .flatMap(lines => lines.split("\\s+"))
                   .map(word =>(word,1L))
                   .reduceByKey((sum, value) => sum + value)

   val repartitionedResult = result.repartition(3)

//   val cachedDataCount = cachedData.count()
   repartitionedResult.saveAsTextFile(Constants.training_output_WC)

}
