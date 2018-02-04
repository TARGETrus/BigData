package com.epam.hubd.spark.scala.operation

import com.epam.hubd.spark.scala.operation.util.Util._

/**
  * Similar to map, but each input item can be mapped to 0 or more output items
  * so the function should return a Seq rather than a single item
  * flatMap process doesn't shuffle the data across the partitions
  *
  * Created by Alexandra_Simon on 7/11/2016.
  */

object FlatMapOp extends ConfSettings {
  def main(args: Array[String]) {

    val rdd = sc.parallelize(
      List("some text here", "other text over there",
        "and text", "short", "and a longer one"), 3)
    val flatmappedRDD = rdd.flatMap(x => x.split("\\s"))

    showRddPartitions(rdd, "inputRDD")
    showRddPartitions(flatmappedRDD, "flatMappedRDD")

  }
}