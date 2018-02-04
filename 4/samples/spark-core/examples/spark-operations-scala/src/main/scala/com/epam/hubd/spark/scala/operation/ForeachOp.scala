package com.epam.hubd.spark.scala.operation

/**
  * Run a function func on each element of the dataset.
  * This is usually done for side effects such as updating an Accumulator or interacting with external storage systems.
  *
  * Created by Kalman_Jantner on 7/11/2016.
  */
object ForeachOp extends ConfSettings {

  def main(args: Array[String]) = {

    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
    rdd.foreach(x => {
      println(s"Next item is $x")
    })
  }
}
