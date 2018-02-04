package com.epam.hubd.spark.scala.operation

import com.epam.hubd.spark.scala.operation.util.Util._

/**
  * Returns a new distributed dataset formed by
  * passing each element of the source through a function.
  *
  * Created by Csaba_Bejan on 8/11/2016.
  */
object FilterOp extends ConfSettings {
  def main(args: Array[String]) {

    val rdd = sc.parallelize(List("a", "b", "c", "d", "e", "f", "x", "y", "z"), 3)
    val filteredRDD = rdd.filter(x => x < "f")

    showRddPartitions(rdd, "rdd")
    showRddPartitions(filteredRDD, "filteredRDD")
  }
}