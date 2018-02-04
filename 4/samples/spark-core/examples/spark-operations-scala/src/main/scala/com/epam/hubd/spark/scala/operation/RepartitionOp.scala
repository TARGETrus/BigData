package com.epam.hubd.spark.scala.operation

import com.epam.hubd.spark.scala.operation.util.Util._

/**
  * Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them.
  * This always shuffles all data over the network.
  *
  * Created by Kalman_Jantner on 7/11/2016.
  */
object RepartitionOp extends ConfSettings {

  def main(args: Array[String]) = {

    val rdd = sc.parallelize(List("a", "b", "c", "d", "e", "f", "g", "h", "i"), 2)
    val repartitionRdd = rdd.repartition(4)

    showRddPartitions(rdd)
    showRddPartitions(repartitionRdd, "repartitionedRdd")
  }
}
