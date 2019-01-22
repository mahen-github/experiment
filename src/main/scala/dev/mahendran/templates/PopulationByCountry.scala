package dev.mahendran.templates

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


/**
  * Country Name	Country Code	2000 [YR2000]	2001 [YR2001]	2002 [YR2002]	2003 [YR2003]	2004 [YR2004]	2005 [YR2005]	2006 [YR2006]	2007 [YR2007]	2008 [YR2008]	2009 [YR2009]	2010 [YR2010]	2011 [YR2011]	2012 [YR2012]	2013 [YR2013]	2014 [YR2014]	2015 [YR2015]
  *
  */
case class Population(Country_Name: String, Country_Code: String,
                      YR2000: String, YR2001: String, YR2002: String, YR2003: String,
                      YR2004: String, YR2005: String, YR2006: String, YR2007: String,
                      YR2008: String, YR2009: String, YR2010: String, YR2011: String,
                      YR2012: String, YR2013: String, YR2014: String, YR2015: String) {
   //   override def toString: String =
   //      s"""${Country_Name},
   //${Country_Code},
   //${YR2000},
   //${YR2001},
   //${YR2002},
   //${YR2003},
   //${YR2004},
   //${YR2005},
   //${YR2006},
   //${YR2007},
   //${YR2008},
   //${YR2009},
   //${YR2010},
   //${YR2011},
   //${YR2012},
   //${YR2013},
   //${YR2014},
   //${YR2015} """
}

class PopulationByCountry(@transient val sc: SparkContext) {

   val hdfsWriter = HdfsWriter.configure(sc)
   def read() = {
      sc.textFile(Constants.dataPath + "/Population.csv")
   }

   def process(data: RDD[String]): RDD[(String, Population)] = {
      val result = data
         .map(lines => lines.split(","))
         .map { line =>
            Population(line(0).replace("\"", ""),
               line(1),
               line(2),
               line(3),
               line(4),
               line(5),
               line(6),
               line(7),
               line(8),
               line(9),
               line(10),
               line(11),
               line(12),
               line(13),
               line(14),
               line(15),
               line(16),
               line(17))

         }.map { population => (population.Country_Name, population) }
      result
   }

   def write(rdd: RDD[(String, Population)], path:String): Unit = {
      val hadoopConf = sc.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)
      if (fs.exists(new Path(path))) {
         fs.delete(new Path(path), true)
      }
      hdfsWriter.writeTextFileByType(rdd, path)
   }
}

object PopulationByCountry extends App {
   val sparkConf = new SparkConf()
   val sc = new SparkContext(sparkConf)
   val populationByCountry = new PopulationByCountry(sc)
   populationByCountry.write(populationByCountry.process(populationByCountry.read()), Constants.output_Population)
}
