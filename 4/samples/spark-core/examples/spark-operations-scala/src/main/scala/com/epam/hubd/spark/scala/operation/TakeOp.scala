package com.epam.hubd.spark.scala.operation

import com.epam.hubd.spark.scala.operation.util.Util._

/**
  * Return an array with the first n elements of the dataset.
  *
  * Created by Kalman_Jantner on 7/11/2016.
  */
object TakeOp extends ConfSettings {

  def main(args: Array[String]) = {

    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
    val take: Array[Int] = rdd.take(3)

    showRddContent(rdd)
    println(take.toList)
  }
}
