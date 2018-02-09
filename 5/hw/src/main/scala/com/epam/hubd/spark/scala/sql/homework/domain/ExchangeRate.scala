package com.epam.hubd.spark.scala.sql.homework.domain

case class ExchangeRate(dateTime: String, rate: Double) {
  override def toString: String = s"$dateTime,$rate"
}
