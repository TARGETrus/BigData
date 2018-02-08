package com.epam.hubd.spark.scala.sql.homework

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.storage.StorageLevel

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val spark = SparkSession
      .builder()
      .appName("motels-home-recommendation")
      .enableHiveSupport()
      .getOrCreate()

    processData(spark, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    spark.stop()
  }

  def processData(spark: SparkSession, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String): Unit = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: DataFrame = getRawBids(spark, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      */
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
    erroneousRecords.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: DataFrame = getExchangeRates(spark, exchangeRatesPath)

    /**
      * Task 3:
      * UserDefinedFunction to convert between date formats.
      * Hint: Check the formats defined in Constants class
      */
    val convertDate: UserDefinedFunction = getConvertDate

    /**
      * Task 3:
      * Transform the rawBids
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: DataFrame = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: DataFrame = getMotels(spark, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names.
      */
    val enriched: DataFrame = getEnriched(bids, motels)
    enriched.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(spark: SparkSession, bidsPath: String): DataFrame = {
    spark
      .read
      .parquet(bidsPath)
      .persist(StorageLevel.MEMORY_ONLY)
  }

  def getErroneousRecords(rawBids: DataFrame): DataFrame = ???

  def getExchangeRates(spark: SparkSession, exchangeRatesPath: String): DataFrame = ???

  def getConvertDate: UserDefinedFunction = ???

  def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = ???

  def getMotels(spark: SparkSession, motelsPath: String): DataFrame = ???

  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = ???
}
