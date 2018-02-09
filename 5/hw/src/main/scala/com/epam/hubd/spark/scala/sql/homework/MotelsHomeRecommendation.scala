package com.epam.hubd.spark.scala.sql.homework

import com.epam.hubd.spark.scala.sql.homework.domain.ExchangeRate
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StructField, _}
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
      * UserDefinedFunction to convert currency.
      */
    val convertPrice: UserDefinedFunction = getConvertPrice

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

    // Uses default read parquet method to create DF.
    // It works, and works fast, still, schema inferred from parquet metadata, (Metadata way)
    // so we can't define value types which are defined as a strings.
    // All column's types inferred are strings.
    val rdd = spark
      .read
      .parquet(bidsPath)
      .persist(StorageLevel.MEMORY_ONLY_SER)

    return rdd
  }

  def getErroneousRecords(rawBids: DataFrame): DataFrame = {

    val rdd = rawBids
      .filter(rawBids("HU").contains("ERROR_"))
      .groupBy(rawBids("BidDate"), rawBids("HU"))
      .agg(count(rawBids("HU")) as "Quantity")

    return rdd
  }

  def getExchangeRates(spark: SparkSession, exchangeRatesPath: String): DataFrame = {

    // To enable '.toDF' implicit transformation inside this function. Global import is not possible at the moment.
    import spark.implicits._

    // Illustrates how to read csv file as DataFrame using Schema Inferring by Reflection. (Programmatic way)
    // For demonstration purposes only. Not a fast way, reflections + transformations used in the process.
    // To read as DF: spark.read.format(Constants.CSV_FORMAT).option("header", "false").load(exchangeRatesPath)
    val rdd = spark
      .sparkContext
      .textFile(exchangeRatesPath)
      .map(_.split(Constants.DELIMITER))
      .map(values => ExchangeRate(values(0), values(3).toDouble))
      .toDF

    return rdd
  }

  def getConvertDate: UserDefinedFunction = udf(
    (dateTime: String) => Constants.INPUT_DATE_FORMAT.parseDateTime(dateTime).toString(Constants.OUTPUT_DATE_FORMAT)
  )

  def getConvertPrice: UserDefinedFunction = udf(
    (currency: String, rate: Double) => "%.3f".format(currency.toDouble * rate).toDouble
  )

  def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = {

    val bids = rawBids
      .filter(!rawBids("HU").contains("ERROR_"))
      .join(exchangeRates, rawBids("BidDate") === exchangeRates("dateTime"), "inner")
      .persist(StorageLevel.MEMORY_ONLY_SER)

    val bidsUS = bids
      .filter(rawBids("US") =!= "")
      .select(rawBids("MotelID"), getConvertDate(rawBids("BidDate")), lit(Constants.TARGET_LOSAS(0)), getConvertPrice(rawBids("US"), exchangeRates("rate")))

    rawBids.unpersist(false)

    return bidsUS
  }

  def getMotels(spark: SparkSession, motelsPath: String): DataFrame = {

    // User-defined motels data schema.
    val schema = new StructType()
      .add(StructField("MotelID", StringType, nullable = false))
      .add(StructField("MotelName", StringType, nullable = false))

    // Here we use schema to override metadata inferring while reading from parquet file. (Programmatic way)
    // Can be used to read only specific columns. In case of column names/types in schema are not corresponding
    // to ones in parquet, we will get null_values/exceptions. Not sure is it possible to re-define value types.
    val rdd = spark
      .read
      .schema(schema)
      .parquet(motelsPath)

    return rdd
  }

  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = ???
}
