package com.epam.hubd.spark.scala.operation

import com.epam.hubd.spark.scala.operation.util.Util._

/**
  * Aggregate the elements of the dataset using a function (which takes two arguments and returns one).
  * The function should be commutative and associative so that it can be computed correctly in parallel.
  *
  * Created by Kalman_Jantner on 7/11/2016.
  */
object ReduceOp extends ConfSettings {

  def main(args: Array[String]) = {

    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
    val sumOfItems: Int = rdd.reduce((x, y) => x + y)

    showRddContent(rdd, "rdd")
    print(s"Sum of numbers in RDD: $sumOfItems")

  }
}
