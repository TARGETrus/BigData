package com.epam.hubd.spark.scala.operation

import com.epam.hubd.spark.scala.operation.util.Util._

/**
  * Aggregate the values of each key, using given combine functions and a neutral "zero value".
  * This function can return a different result type, U, than the type of the values in this RDD,
  * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
  * as in scala.TraversableOnce. The former operation is used for merging values within a
  * partition, and the latter is used for merging values between partitions. To avoid memory
  * allocation, both of these functions are allowed to modify and return their first argument
  * instead of creating a new U.
  *
  * Created by Alexandra_Simon on 7/11/2016.
  */
object AggregateByKeyOp extends ConfSettings {
  def main(args: Array[String]) {

    println("String concat based example:")
    val rdd = sc.parallelize(
      List((1, "10"), (2, "20"), (3, "30"), (2, "21"), (3, "31"), (4, "40"),
        (1, "11"), (1, "12"), (2, "22"), (1, "23"), (3, "33"), (2, "23"), (4, "41"), (3, "33")), 3)

    val aggregatebykeyRDD =
      rdd.aggregateByKey("ZeroItem")((a, b) => b + ";" + a, (a, b) => a + "|" + b)

    showRddContent(rdd)
    showRddPartitions(rdd)

    showRddContent(aggregatebykeyRDD)
    showRddPartitions(aggregatebykeyRDD)

    println("\nSum string numbers example:")
    val rdd2 = sc.parallelize(
      List((1, "10"), (2, "20"), (3, "30"), (2, "21"), (3, "31"), (4, "40"),
        (1, "11"), (1, "12"), (2, "22"), (1, "23"), (3, "33"), (2, "23"), (4, "41"), (3, "33")), 3)

    showRddContent(rdd2)
    showRddPartitions(rdd2)

    val aggregatebykeyRDD2 =
      rdd2.aggregateByKey(1000)((a, b) => a + b.toInt, (a, b) => a + b)

    showRddContent(aggregatebykeyRDD2, "rdd2")
    showRddPartitions(aggregatebykeyRDD2, "rdd2")
  }
}
