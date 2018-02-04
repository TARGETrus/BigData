package com.epam.hubd.spark.scala.operation

import com.epam.hubd.spark.scala.operation.util.Util._

import scala.reflect.io.Path
import scala.util.Try

/**
  * saveAsText:
  * Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system.
  * Spark will call toString on each element to convert it to a line of text in the file.
  *
  * saveAsObjectFile:
  * Write the elements of the dataset in a simple format using Java serialization, which can then be loaded using SparkContext.objectFile().
  *
  * Created by Kalman_Jantner on 7/11/2016.
  */
object SaveAsTextFileOp extends ConfSettings {

  def main(args: Array[String]) = {
    val textFilePath: String = "./target/tmp/txt/rdd_saveAsTextFile"
    val objectFilePath: String = "./target/tmp/object/rdd_saveAsObjectFile"

    cleanup()

    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    showRddContent(rdd)

    println(s"Save to: $textFilePath")
    rdd.saveAsTextFile(textFilePath)

    println(s"Save to: $objectFilePath")
    rdd.saveAsObjectFile(objectFilePath)

    val objectFileRdd = sc.objectFile[Int](objectFilePath)
    val textRdd = sc.textFile(textFilePath)

    println(s"\nData read from: $objectFilePath")
    showRddContent(objectFileRdd, "objectFileRdd")

    println(s"Data read from:$textFilePath")
    showRddContent(textRdd, "textRdd")
  }

  def cleanup(): Unit = {
    Try(Path("./target/tmp/").deleteRecursively())
  }
}
