package com.epam.hubd.spark.scala.operation

import com.epam.hubd.spark.scala.operation.util.Util._

/**
  * Decrease the number of partitions in the RDD to numPartitions.
  * Useful for running operations more efficiently after filtering down a large dataset.
  * Shuffle can be controller via the second optional parameter.
  * Cannot increase number of partition.
  *
  * Created by Kalman_Jantner on 7/11/2016.
  */
object CoalesceOp extends ConfSettings {

  def main(args: Array[String]) = {

    val rdd = sc.parallelize(List("a", "b", "c", "d", "e", "f", "g", "h", "i"), 3)

    showRddPartitions(rdd)
    showRddPartitions(rdd.coalesce(2), "coalesceRdd")

    println("\nCannot increase number of partition!")
    showRddPartitions(rdd)
    showRddPartitions(rdd.coalesce(6), "coalesceRddWithNumberOfPartition6Parameter")
  }
}
