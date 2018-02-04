package com.epam.hubd.spark.scala.operation

import com.epam.hubd.spark.scala.operation.util.Util._

/**
  * Sort the elements by key
  * It is good if you want to order your elements
  * sortByKey process shuffle the data across the partitions
  * sortByKey() //desc
  * sortByKey(true) //desc
  * sortByKey(false) //asc
  *
  * Created by Alexandra_Simon on 7/11/2016.
  */

object SortByKeyOp extends ConfSettings {
  def main(args: Array[String]) {

    val list = sc.parallelize(List(("this", 1), ("is", 1), ("the", 1), ("is", 1), ("word", 1), ("where", 1), ("a", 1), ("dog", 1), ("goes", 1)), 4)

    showRddPartitions(list, "inputRDD")
    showRddPartitions(list.sortByKey(true), "sortedRDD")

  }
}