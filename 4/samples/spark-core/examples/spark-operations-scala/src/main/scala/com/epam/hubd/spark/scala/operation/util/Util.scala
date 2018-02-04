package com.epam.hubd.spark.scala.operation.util

import org.apache.spark.rdd.RDD

/**
  * Util class for print rdd content on console.
  * Created by Kalman_Jantner on 7/12/2016.
  */
object Util {

  def showRdd[T](rdd: RDD[T]): Unit = {
    println(rdd.glom().collect().map(x => x.toList).toList)
  }

  def showRddPartitions[T](rdd: RDD[T]): Unit = {
    showRddPartitions(rdd, "rdd")
  }

  def showRddPartitions[T](rdd: RDD[T], rddName: String): Unit = {
    println(s"Content of [$rddName] per partition:")
    rdd.glom().collect().map(x => x.toList.mkString(",")).toList.foreach(x => {
      println(s"    Partition: $x")
    })
  }

  def showRddContent[T](rdd: RDD[T]): Unit = {
    showRddContent(rdd, "rdd")
  }

  def showRddContent[T](rdd: RDD[T], rddName: String): Unit = {
    val data = (rdd.collect().mkString(", "))
    println(s"Content of [$rddName]: $data")
  }

  def showRddCompactBuffer[T](rdd: RDD[T]): Unit = {
    showRddCompactBuffer(rdd, "rdd")
  }

  def showRddCompactBuffer[T](rdd: RDD[T], rddName: String): Unit = {
    println(s"Content of [$rddName] per partition:")
    rdd.glom().collect().map(x => x.toList.mkString(",")).toList.foreach(x => {
      val xmodified = x.replaceAll("CompactBuffer", "")
      println(s"    Partition: $xmodified")
    })
  }

}
