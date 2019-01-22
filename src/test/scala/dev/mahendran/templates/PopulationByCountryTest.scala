package dev.mahendran.templates

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.hadoop.fs.Path
import org.apache.hadoop.test.PathUtils
import org.apache.log4j.{Level, Logger}
import org.scalatest.FunSpec

class PopulationByCountryTest extends  FunSpec with SharedSparkContext{


   var path: Path = _

   override def beforeAll(): Unit = {

      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

      conf.setMaster("local[1]")
      super.beforeAll()
      path = new Path(PathUtils.getTestDirName(classOf[PopulationByCountryTest]))
      sc.hadoopConfiguration.set("fs.defaultFS", "file:///" + path)
      Logger.getRootLogger.setLevel(Level.WARN)
   }

   override def afterAll(): Unit = super.afterAll()

   describe("when there is pupulation data"){
      it("write data by country"){

         val output = path + "/output"
         val inPath = ClassLoader.getSystemResource("data_demo/Population.csv").getPath
         val rdd = sc.textFile(inPath)
         val pc =new PopulationByCountry(sc)
         val result = pc.process(rdd)
         println(output)
         pc.write(result, output)
      }
   }
}
