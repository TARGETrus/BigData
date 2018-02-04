package com.epam.hubd.spark.scala.operation

import com.epam.hubd.spark.scala.operation.util.Util._

/**
  * Return all the elements of the dataset as an array at the driver program.
  * This is usually useful after a filter or other operation that returns a sufficiently small subset of the data.
  * Created by Kalman_Jantner on 7/11/2016.
  */
object CollectOp extends ConfSettings {

  def main(args: Array[String]) = {

    val rdd = sc.parallelize(List("a", "b", "c", "d", "e", "f", "g", "h", "i"))
    val collectResult: Array[String] = rdd.collect()

    showRddContent(rdd)
    println("Result Class: " + collectResult.getClass)
    println("Result content as list: " + collectResult.toList)
  }
}
