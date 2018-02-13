package com.epam.hubd.spark.scala.sql.homework

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object Constants {

  val DELIMITER = ","

  val BIDS_HEADER = Seq("MotelID", "BidDate", "HU", "UK",  "NL", "US", "MX", "AU", "CA", "CN", "KR","BE", "I","JP", "IN", "HN", "GY", "DE")
  val MOTELS_HEADER = Seq("MotelID", "MotelName", "Country", "URL", "Comment")
  val CUSTOM_HEADER = Seq("Quantity", "LoSa", "Price")

  val CSV_FORMAT = "com.databricks.spark.csv"

  val EXCHANGE_RATES_HEADER = StructType(Array("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")
    .map(field => StructField(field, StringType, true)))

  val TARGET_LOSAS = Seq("US", "CA", "MX")

  val INPUT_DATE_FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("HH-dd-MM-yyyy")
  val OUTPUT_DATE_FORMAT: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
}
