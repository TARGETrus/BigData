package com.epam.hubd.spark.scala.operation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Alexandra_Simon on 7/19/2016.
  */
trait ConfSettings {
  val sc = createContext

  private def createContext = {
    val conf = new SparkConf().setAppName("distinctExample").setMaster("local[2]")
    new SparkContext(conf)
  }
}
