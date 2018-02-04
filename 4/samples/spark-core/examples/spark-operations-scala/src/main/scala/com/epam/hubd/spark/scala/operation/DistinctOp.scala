package com.epam.hubd.spark.scala.operation

import com.epam.hubd.spark.scala.operation.util.Util._

/**
  * Return a new dataset that contains the distinct elements of the dataset
  * It is good if your data is, for example, duplicated
  * distinct process shuffle the data across the partitions
  *
  * Created by Alexandra_Simon on 7/11/2016.
  */

object DistinctOp extends ConfSettings {
  def main(args: Array[String]) {

    val list = sc.parallelize(List("a", "bbb", "cc", "cc", "cc", "bbb", "bbb"), 5)

    showRddPartitions(list, "inputRDD")
    showRddPartitions(list.distinct, "distinctedRDD")

  }
}

