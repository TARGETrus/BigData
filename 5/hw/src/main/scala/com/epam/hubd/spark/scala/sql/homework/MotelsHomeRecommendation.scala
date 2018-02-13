package com.epam.hubd.spark.scala.sql.homework

import com.epam.hubd.spark.scala.sql.homework.domain.ExchangeRate
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
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
    erroneousRecords
      .coalesce(1) // ONLY FOR SMALL INPUT FILES! (few GBs at most) Added for convenient HW output files.
      .write
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
    enriched
      .coalesce(1) // ONLY FOR SMALL INPUT FILES! (few GBs at most) Added for convenient HW output files.
      .write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  /**
    * Reads bids parquet file using DataSource API.
    *
    * @param spark current spark session object.
    * @param bidsPath path to bids (parquet) file.
    * @return DataFrame representation of bids file contents.
    */
  def getRawBids(spark: SparkSession, bidsPath: String): DataFrame = {

    // Uses default read parquet method to create DF.
    // It works, and works fast, still, schema inferred from parquet metadata, (Metadata way)
    // so we can't define value types which are defined as a strings.
    // All column's types inferred are strings.
    spark
      .read
      .parquet(bidsPath)
      .persist(StorageLevel.MEMORY_ONLY_SER)
  }

  /**
    * Reads erroneous records and groups them by date and error type.
    *
    * @param rawBids DataFrame with bids data.
    * @return DataFrame representation of erroneous bids records.
    */
  def getErroneousRecords(rawBids: DataFrame): DataFrame = {

    rawBids
      .filter(rawBids("HU").contains("ERROR_"))
      .groupBy(rawBids("BidDate"), rawBids("HU"))
      .agg(count(rawBids("HU")) as "Quantity")
  }

  /**
    * Reads exchange rates csv file using sparkContext and domain object, then parsed to DataFrame.
    * Implemented this way to illustrate one of the possible approaches to DataFrame creations.
    *
    * @param spark current spark session object.
    * @param exchangeRatesPath path to exchange rates (csv) file.
    * @return DataFrame representation of exchange rates file contents.
    */
  def getExchangeRates(spark: SparkSession, exchangeRatesPath: String): DataFrame = {

    // To enable '.toDF' implicit transformation inside this function. Global import is not possible at the moment.
    import spark.implicits._

    // Illustrates how to read csv file as DataFrame using Schema Inferring by Reflection. (Programmatic way)
    // For demonstration purposes only. Not a fast way, reflections + transformations used in the process.
    // To read as DF: spark.read.format(Constants.CSV_FORMAT).option("header", "false").load(exchangeRatesPath)
    spark
      .sparkContext
      .textFile(exchangeRatesPath)
      .map(_.split(Constants.DELIMITER))
      .map(values => ExchangeRate(values(0), values(3).toDouble))
      .toDF
  }

  /**
    * User-defined function to parse date-time column value.
    *
    * @return handled date-time column.
    */
  def getConvertDate: UserDefinedFunction = udf(
    (dateTime: String) => Constants.INPUT_DATE_FORMAT.parseDateTime(dateTime).toString(Constants.OUTPUT_DATE_FORMAT)
  )

  /**
    * User-defined function to convert currency column value by provided rate.
    *
    * @return converted currency value.
    */
  def getConvertPrice: UserDefinedFunction = udf(
    (currency: String, rate: Double) => "%.3f".format(currency.toDouble * rate).toDouble
  )

  /**
    * Filters bids with erroneous and empty price values,
    * explodes correct bids by country,
    * formats date,
    * adds Losa,
    * converts price from USD to EUR.
    *
    * @param rawBids DataFrame with bids data.
    * @param exchangeRates DataFrame with currency exchange rates data.
    * @return bids DataFrame filtered errors, exploded by countries, parsed date-time, converted currencies and LoSa.
    */
  def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = {

    val bids = rawBids
      .filter(!rawBids("HU").contains("ERROR_"))
      .join(exchangeRates, rawBids("BidDate") === exchangeRates("dateTime"), "inner")
      .persist(StorageLevel.MEMORY_ONLY_SER)

    val bidsUS = bids
      .filter(bids("US") =!= "")
      .select(
        bids("MotelID"), getConvertDate(bids("BidDate")) as "BidDate", lit(Constants.TARGET_LOSAS.head) as "LoSa",
        getConvertPrice(bids("US"), exchangeRates("rate")) as "Price"
      )

    val bidsMX = bids
      .filter(bids("CA") =!= "")
      .select(
        bids("MotelID"), getConvertDate(bids("BidDate")) as "BidDate", lit(Constants.TARGET_LOSAS(1)) as "LoSa",
        getConvertPrice(bids("CA"), exchangeRates("rate")) as "Price"
      )

    val bidsCA = bids
      .filter(bids("MX") =!= "")
      .select(
        bids("MotelID"), getConvertDate(bids("BidDate")) as "BidDate", lit(Constants.TARGET_LOSAS(2)) as "LoSa",
        getConvertPrice(bids("MX"), exchangeRates("rate")) as "Price"
      )

    val bidsByCountries = bidsUS
      .union(bidsMX)
      .union(bidsCA)

      /* Tried to use explode, not enough control to carry out the task.
      .select(
        rawBids("MotelID"), getConvertDate(rawBids("BidDate")) as "BidDate",
        explode(
          array(
            when(rawBids("US") =!= "", getConvertPrice(rawBids("US"), exchangeRates("rate"))),
            when(rawBids("MX") =!= "", getConvertPrice(rawBids("MX"), exchangeRates("rate"))),
            when(rawBids("CA") =!= "", getConvertPrice(rawBids("CA"), exchangeRates("rate")))
          )
        ) as "Price"
      )
      .where(col("Price").isNotNull)
      */

    rawBids.unpersist(false)
    bids.unpersist(false)

    return bidsByCountries
  }

  /**
    * Reads motels file using DataSource API and user-defined schema.
    *
    * @param spark current spark session object.
    * @param motelsPath path to motels (parquet) file.
    * @return DataFrame representation of motels file contents.
    */
  def getMotels(spark: SparkSession, motelsPath: String): DataFrame = {

    // User-defined motels data schema.
    val schema = StructType(Seq(
      StructField("MotelID", StringType, nullable = false),
      StructField("MotelName", StringType, nullable = false)
    ))

    // Here we use schema to override metadata inferring while reading from parquet file. (Programmatic way)
    // Can be used to read only specific columns. In case of column names/types in schema are not corresponding
    // to ones in parquet, we will get null_values/exceptions. Not sure is it possible to re-define value types.
    spark
      .read
      .schema(schema)
      .parquet(motelsPath)
  }

  /**
    * Enriches bids data with motels names,
    * filters them by multiple highest prices for each motel date-time and id combination.
    *
    * @param bids parsed bids DataFrame.
    * @param motels motels info DataFrame.
    * @return DataFrame containing bids enriched with motels info and filtered by highest prices.
    */
  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = {

    val window = Window.partitionBy(bids("MotelID"), bids("BidDate"))

    bids
      .join(motels, bids("MotelID") === motels("MotelID"))
      .withColumn("PriceMax", max(bids("Price")) over window)
      .select(bids("MotelID"), motels("MotelName"), bids("BidDate"), bids("LoSa"), bids("Price"))
      .where(bids("Price") === col("PriceMax"))
  }
}
