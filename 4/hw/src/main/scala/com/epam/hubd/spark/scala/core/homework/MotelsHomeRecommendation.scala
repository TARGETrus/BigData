package com.epam.hubd.spark.scala.core.homework

import com.epam.hubd.spark.scala.core.homework.domain.{BidError, BidItem, EnrichedItem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation"))

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String): Unit = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BidError case class
      */
    val erroneousRecords: RDD[String] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    val enriched:RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  /**
    * Read bids file and construct rdd with List of lines.
    *
    * @param sc current spark context.
    * @param bidsPath path to file with bids data.
    * @return rdd with list of lines.
    */
  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    sc.textFile(bidsPath).map(line => line.split(",").toList)
  }

  /**
    * Extracts all erroneous records from bids rdd, counts them by unique dates and error types.
    *
    * @param rawBids bids data in form of rdd with List of lines.
    * @return rdd with erroneous records.
    */
  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = {
    rawBids
      .filter(line => line(2).contains("ERROR_"))
      .map(line => (BidError(line(1), line(2)), 1))
      .reduceByKey((a, b) => a + b)
      .map(v => v._1 + "," + v._2)
  }

  /**
    * Extracts exchange rates history with date and value in Map.
    *
    * @param sc current spark context.
    * @param exchangeRatesPath path to file with exchange rates data.
    * @return Map packed with string dates as a key and exchange rate as value.
    */
  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = {
    sc
      .textFile(exchangeRatesPath)
      .map(line => {
        val splitted = line.split(",")
        (splitted(0), splitted(3).toDouble)
      })
      .collect()
      .toMap
  }

  /**
    * Filters bids with erroneous and empty price values,
    * explodes correct bids by country,
    * formats date,
    * converts price from USD to EUR.
    *
    * @param rawBids bids data in form of rdd with List of lines.
    * @param exchangeRates Map packed with string dates as a key and exchange rate as value.
    * @return rdd with filled BidItems.
    */
  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {
    rawBids
      .filter(line => {
        !line(2).contains("ERROR_")
      })
      .flatMap(line => {
        val rate: Double = exchangeRates(line(1))
        val date = Constants.INPUT_DATE_FORMAT.parseDateTime(line(1)).toString(Constants.OUTPUT_DATE_FORMAT)

        val bidItems = ListBuffer.empty[BidItem]

        if (line(5).nonEmpty) bidItems += BidItem(line(0), date, Constants.TARGET_LOSAS(0), currExchange(line(5), rate))
        if (line(8).nonEmpty) bidItems += BidItem(line(0), date, Constants.TARGET_LOSAS(1), currExchange(line(8), rate))
        if (line(6).nonEmpty) bidItems += BidItem(line(0), date, Constants.TARGET_LOSAS(2), currExchange(line(6), rate))

        bidItems.toList
      })
  }

  /**
    * Read bids file and construct rdd with motels id/name.
    *
    * @param sc current spark context.
    * @param motelsPath path to file with motels data.
    * @return rdd with motels info.
    */
  def getMotels(sc:SparkContext, motelsPath: String): RDD[(String, String)] = {
    sc
      .textFile(motelsPath)
      .map(line => {
        val splitted = line.split(",")
        (splitted(0), splitted(1))
      })
  }

  /**
    * Finds highest price for each hotel in rdd.
    *
    * @param bids rdd with filled BidItems.
    * @param motels rdd with filled motels data.
    * @return rdd with filled EnrichedItems.
    */
  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {
    // We'll have to make some transformations to make join possible. mb a way to make it prettier?
    bids
      .groupBy(bidItem => (bidItem.motelId, bidItem.bidDate))
      .map(bidItems => bidItems._2.maxBy(_.price))
      .map(bidItem => (bidItem.motelId, bidItem))
      .join(motels)
      .map(joined => EnrichedItem(joined._1, joined._2._2, joined._2._1.bidDate, joined._2._1.loSa, joined._2._1.price))
  }

  /**
    * Converts currencies by rate, 3 decimals precision.
    * Not included in BidItem constructor because of it's specific use.
    *
    * @param currency incoming value
    * @param rate exchange rate
    * @return Double excanged currency
    */
  private def currExchange(currency: String, rate: Double): Double = {
    "%.3f".format(currency.toDouble * rate).toDouble
  }

}
