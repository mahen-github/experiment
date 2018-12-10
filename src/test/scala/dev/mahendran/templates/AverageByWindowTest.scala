package dev.mahendran.templates

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

class AverageByWindowTest extends FunSuite with SharedSparkContext {

  override def beforeAll() {
    super.beforeAll()
  }

  override def afterAll() {
    super.afterAll()
  }

  ignore("Gettign the average by window") {
    val windowSize = 3
//        val numbers = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val numbers = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100)
    val actualNumbers  = new AverageByWindow(sc).computeNumbers(numbers,windowSize)
    val expectedNumbers = Seq.range(2, 100)
    assert(actualNumbers==expectedNumbers)
    
  val num = s""" 53 15 57 71 86 83 53 56 15 78 28 24 65 89 80 82 91 24 22 8 100 30 29 1 87
13 40 59 2 2 83 82 29 91 1 68 99 29 88 67 68 69 37 36 68 31 2 99 20 18 3 7 77 48
82 45 17 71 28 89 41 3 31 8 68 12 74 89 12 83 63 98 51 82 38 11 81 38 85 15 15
5839 61 16 83 12 27 26 74 7 18 77 41 33 84 94 22 49 49 """
  num.split(",").map { x => x.asInstanceOf[Int] }.toSeq
  }
  
  test("Getting the average by window random numbers") {
    val windowSize = 3
    val num = Seq(53,15,57,71,86,83,53,56,15,78,28,24,65,89,80,82,91,24,22,8,100,30,29,1,87,13,40,59,2,2,83,82,29,91,1,68,99,29,88,67,68,69,37,36,68,31,2,99,20,18,3,7,77,48,82,45,17,71,28,89,41,3,31,8,68,12,74,89,12,83,63,98,51,82,38,11,81,38,85,15,15,58,39,61,16,83,12,27,26,74,7,18,77,41,33,84,94,22,49,49)
    val actualNumbers  = new AverageByWindow(sc).computeNumbers(num,windowSize)
    actualNumbers foreach print
    
//    val expectedNumbers = Seq.range(2, 100)
//    assert(actualNumbers==expectedNumbers)
    
  
  }
}
