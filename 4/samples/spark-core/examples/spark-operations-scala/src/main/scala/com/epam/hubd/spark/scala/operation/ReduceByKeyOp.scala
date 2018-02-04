package com.epam.hubd.spark.scala.operation

import com.epam.hubd.spark.scala.operation.util.Util._

/**
  * Reduce the values by key
  * It is good if you want to add a speacial rule to reduce (for example: sum) the values
  * reduceByKey process shuffle the data across the partitions
  * You can set in param how many partitions would you like to have finally
  *
  * Created by Alexandra_Simon on 7/11/2016.
  */

object ReduceByKeyOp extends ConfSettings {
  def main(args: Array[String]) {

    val list = sc.parallelize(List(("this", 1), ("is", 1), ("the", 1), ("is", 1), ("word", 1), ("is", 1), ("the", 1), ("is", 1), ("word", 1)), 3)

    showRddPartitions(list, "inputRDD")
    showRddPartitions(list.reduceByKey((v1, v2) => v1 + v2, 4), "reducedRDD")

  }
}
