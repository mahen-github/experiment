package dev.mahendran.templates

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class AverageByWindow(@transient val sc: SparkContext) {

  def computeNumbers(numbers: Seq[Int], windowSize: Int): Seq[Double] = {
    val rddofNum = sc.parallelize(numbers, 4)

    val tempRes = rddofNum.zipWithIndex()

    val mapValueWithWindow = tempRes.map(f => {
      (f._1, (f._2, f._2 - 1, f._2 - 2))
    })

    val winWithVaNW = mapValueWithWindow.flatMap {
      case (value, window) =>
        {
          window.productIterator.filter(_.asInstanceOf[Long] >= 0).map { v => (value, v) }.toSeq
        }
    }.groupBy(_._2)

    val winAndValues = winWithVaNW.map {
      case (win, vnw) =>
        (win, vnw.map(_._1))
    }
    val res = winAndValues.filter(_._2.size == 3)map {
      case (win, values) => {
         (win, (values.reduce(_ + _).toFloat/ values.size))
        }
    }
    res.sortBy(_._1.asInstanceOf[Long], true, 1).map(_._2).collect() foreach println
    Seq(0.0)
  }
}

object AverageByWindow extends App {
  val conf = new SparkConf().setAppName("AverageBywindow")
  val sc = new SparkContext(conf)
}
