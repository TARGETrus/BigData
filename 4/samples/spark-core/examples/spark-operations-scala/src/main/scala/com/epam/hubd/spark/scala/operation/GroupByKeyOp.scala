package com.epam.hubd.spark.scala.operation

import com.epam.hubd.spark.scala.operation.util.Util._

/**
  * Group the values by key
  * It is good if you want to collect the values
  * groupByKey process shuffle the data across the partitions and in param (here 5) you can set
  * how many partition would you like to have finally
  * The value at the end will be iterable
  *
  * Created by Alexandra_Simon on 7/11/2016.
  */

object GroupByKeyOp extends ConfSettings {
  def main(args: Array[String]) {

    val list = sc.parallelize(List(("1", "a"), ("1", "b"), ("3", "c"), ("2", "d"), ("3", "e"), ("1", "g"), ("1", "k"), ("3", "m"), ("3", "n"), ("2", "o"), ("3", "p"), ("1", "q"), ("1", "w"), ("3", "x"), ("3", "z")), 5)

    showRddPartitions(list, "inputRDD")
    showRddCompactBuffer(list.groupByKey(5), "groupedByKeyRDD")

  }
}