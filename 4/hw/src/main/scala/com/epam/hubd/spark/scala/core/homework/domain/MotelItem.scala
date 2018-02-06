package com.epam.hubd.spark.scala.core.homework.domain

case class MotelItem(motelId: String, motelName: String) {

  override def toString: String = s"$motelId,$motelName"
}
