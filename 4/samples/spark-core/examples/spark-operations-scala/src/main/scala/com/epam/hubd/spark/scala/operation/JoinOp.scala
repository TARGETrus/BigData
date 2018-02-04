package com.epam.hubd.spark.scala.operation

import com.epam.hubd.spark.scala.operation.util.Util._

/**
  * When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key.
  * Outer joins are supported through leftOuterJoin, rightOuterJoin, and fullOuterJoin.
  *
  * Created by Kalman_Jantner on 7/11/2016.
  */
object JoinOp extends ConfSettings {

  def main(args: Array[String]) = {

    val rdd1 = sc.parallelize(List((1, "a"), (2, "b"), (3, "c")))
    val rdd2 = sc.parallelize(List((2, "B"), (1, "A"), (3, "C")))
    val joinedRdd = rdd1.join(rdd2)

    showRddPartitions(rdd1, "rdd1")
    showRddPartitions(rdd2, "rdd2")
    showRddPartitions(joinedRdd, "joined")
  }
}
