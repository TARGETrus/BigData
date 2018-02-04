package com.epam.hubd.spark.scala.operation

import com.epam.hubd.spark.scala.operation.util.Util._

/**
  * Union the RDDs
  * It is good if you want union two rdds into one.
  * It is the same as operator "++".
  * Unlike "Union" in SQL, here it will not de-duplicate.
  *
  * Created by Alexandra_Simon on 7/11/2016.
  */

object UnionOp extends ConfSettings {
  def main(args: Array[String]) {

    val first = sc.parallelize(List("a bbb cc", "111111 11 1", "xxx yyyyy zzzz"))
    val second = sc.parallelize(List("A BBB CC", "111111 11 1", "XXX YYYYY ZZZZ"))

    val firstmapRDD = first.flatMap(x => x.split(" "))
    val secondmapRDD = second.flatMap(x => x.split(" "))

    showRddContent(firstmapRDD, "inputRDD 1")
    showRddContent(secondmapRDD, "inputRDD 2")

    showRddContent((firstmapRDD union secondmapRDD), "unionOfRDD")

  }
}