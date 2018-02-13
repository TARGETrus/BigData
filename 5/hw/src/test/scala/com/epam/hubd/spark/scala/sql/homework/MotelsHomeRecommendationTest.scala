package com.epam.hubd.spark.scala.sql.homework

import java.io.File

import com.epam.hubd.spark.scala.sql.util.RddComparator
import com.epam.hubd.spark.scala.sql.homework.MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR}
import com.epam.hubd.spark.scala.sql.homework.MotelsHomeRecommendationTest.{sparkSession, _}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructField, _}
import org.junit._
import org.junit.rules.TemporaryFolder
import org.scalatest.FunSuite

class MotelsHomeRecommendationTest extends FunSuite with DataFrameSuiteBase {

  val _temporaryFolder = new TemporaryFolder

  @Rule
  def temporaryFolder: TemporaryFolder = _temporaryFolder

  val BIDS_DIR   = "bids"
  val RATES_DIR  = "rates"
  val MOTELS_DIR = "motels"

  val INPUT_RATES_SAMPLE = "src/test/resources/exchange_rates_sample.txt"

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.gz.parquet"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.gz.parquet"
  val INPUT_BIDS_INTEGRATION_TXT = "src/test/resources/integration/input/bids.txt"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/expected_sql"

  private var outputFolder: File = _

  @Before
  def setup(): Unit = {
    outputFolder = temporaryFolder.newFolder("output")
  }

  @Test
  def shouldReadRawBids(): Unit = {

    // EXPECTED TEST DATA
    // Raw Bids
    // Create test data RDD, we have to go long way because of input parquet file schema.
    val expectedRDD = sparkSession.sparkContext
      .parallelize(Seq(
          List("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL"),
          List("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35")
      ))
      .map(line => Row(line))

    // Create schema equal to homework bids.gz.parquet input file schema.
    val expectedSchema = StructType(Seq(
      StructField("value", ArrayType(StringType, true), nullable = true)
    ))

    // Create expected DF with correct schema.
    val expected = sparkSession.createDataFrame(expectedRDD, expectedSchema)

    // Save DF as parquet.
    expected
      .write
      .parquet(getOutputPath(BIDS_DIR))

    // TEST EXECUTION
    // Read saved parquet file and pass it through spark app.
    val rawBids = MotelsHomeRecommendation.getRawBids(sparkSession, getOutputPath(BIDS_DIR))
    // Make sure file read have the same structure and content as written DF.
    assertDataFrameEquals(expected, rawBids)
  }

  @Test
  def shouldCollectErroneousRecords(): Unit = {

    // INPUT TEST DATA
    // Raw Bids
    val rawBidsRDD = sparkSession.sparkContext
      .parallelize(Seq(
        List("0000001", "06-05-02-2016", "ERROR_1"),
        List("0000002", "15-04-08-2016", "0.89"),
        List("0000003", "07-05-02-2016", "ERROR_2"),
        List("0000004", "06-05-02-2016", "ERROR_1"),
        List("0000005", "06-05-02-2016", "ERROR_2")
      ))
      .map(line => Row.fromSeq(line))

    val rawBidsSchema = StructType(Seq(
      StructField(Constants.BIDS_HEADER(0), StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(1), StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(2), StringType, nullable = true)
    ))

    val rawBids = sparkSession.createDataFrame(rawBidsRDD, rawBidsSchema)

    // EXPECTED TEST DATA
    // Erroneous Records
    val expectedRDD = sparkSession.sparkContext
      .parallelize(Seq(
        List("06-05-02-2016", "ERROR_2", 1L),
        List("07-05-02-2016", "ERROR_2", 1L),
        List("06-05-02-2016", "ERROR_1", 2L)
      ))
      .map(line => Row.fromSeq(line))

    val expectedSchema = StructType(Seq(
      StructField(Constants.BIDS_HEADER(1),   StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(2),   StringType, nullable = true),
      StructField(Constants.CUSTOM_HEADER(0), LongType,   nullable = false)
    ))

    val expected = sparkSession.createDataFrame(expectedRDD, expectedSchema)

    // TEST EXECUTION
    val erroneousRecords = MotelsHomeRecommendation.getErroneousRecords(rawBids)
    assertDataFrameEquals(expected, erroneousRecords)
  }

  @Test
  def shouldReadExchangeRates(): Unit = {

    // EXPECTED TEST DATA
    // Exchange Rates
    val expectedRDD = sparkSession.sparkContext
      .parallelize(Seq(
        List("11-06-05-2016", 0.803),
        List("11-05-08-2016", 0.873),
        List("10-06-11-2015", 0.987)
      ))
      .map(line => Row.fromSeq(line))

    val expectedSchema = StructType(Seq(
      StructField("dateTime", StringType, nullable = true),
      StructField("rate",     DoubleType, nullable = false)
    ))

    val expected = sparkSession.createDataFrame(expectedRDD, expectedSchema)

    // TEST EXECUTION
    val rates = MotelsHomeRecommendation.getExchangeRates(sparkSession, INPUT_RATES_SAMPLE)
    assertDataFrameEquals(expected, rates)
  }

  @Test
  def shouldFilterParseAndExplodeBids(): Unit = {

    // INPUT TEST DATA
    // Raw Bids
    val rawBidsRDD = sparkSession.sparkContext
      .parallelize(Seq(
        List("0000004", "15-04-08-2016",
          "", "1.62", "0.70", "", "1.64", "0.62", "1.08", "", "", "0.70", "", "0.57", "0.61", "0.92", "1.87"),
        List("0000001", "06-05-02-2016",
          "ERROR_NO_BIDS_FOR_HOTEL", "", "", "", "", "", "", "", "", "", "", "", "", "", ""),
        List("0000005", "15-17-08-2016",
          "0.45", "", "0.35", "0.42", "1.21", "1.39", "", "0.44", "1.92", "1.21", "1.54", "1.58", "0.90", "1.22", "0.72", "1.53"),
        List("0000003", "23-04-02-2016",
          "ERROR_BID_SERVICE_TIMEOUT", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
      ))
      .map(line => Row.fromSeq(line))

    val rawBidsSchema = StructType(Seq(
      StructField(Constants.BIDS_HEADER(0),  StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(1),  StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(2),  StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(3),  StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(4),  StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(5),  StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(6),  StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(7),  StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(8),  StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(9),  StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(10), StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(11), StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(12), StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(13), StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(14), StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(15), StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(16), StringType, nullable = true)
    ))

    val rawBids = sparkSession.createDataFrame(rawBidsRDD, rawBidsSchema)

    // Exchange Rates
    val ratesRDD = sparkSession.sparkContext
      .parallelize(Seq(
        List("15-04-08-2016", 0.803),
        List("15-17-08-2016", 0.987)
      ))
      .map(line => Row.fromSeq(line))

    val ratesSchema = StructType(Seq(
      StructField("dateTime", StringType, nullable = true),
      StructField("rate",     DoubleType, nullable = false)
    ))

    val rates = sparkSession.createDataFrame(ratesRDD, ratesSchema)

    // EXPECTED TEST DATA
    // Bids
    val expectedRDD = sparkSession.sparkContext
      .parallelize(Seq(
        List("0000005", "2016-08-17 15:00", "US", 0.415),
        List("0000004", "2016-08-04 15:00", "CA", 0.867),
        List("0000004", "2016-08-04 15:00", "MX", 1.317),
        List("0000005", "2016-08-17 15:00", "MX", 1.194)
      ))
      .map(line => Row.fromSeq(line))

    val expectedSchema = StructType(Seq(
      StructField(Constants.BIDS_HEADER(0),   StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(1),   StringType, nullable = true),
      StructField(Constants.CUSTOM_HEADER(1), StringType, nullable = false),
      StructField(Constants.CUSTOM_HEADER(2), DoubleType, nullable = true)
    ))

    val expected = sparkSession.createDataFrame(expectedRDD, expectedSchema)

    // TEST EXECUTION
    val bids = MotelsHomeRecommendation.getBids(rawBids, rates)
    assertDataFrameEquals(expected, bids)
  }

  @Test
  def shouldReadMotelsInfo(): Unit = {

    // EXPECTED TEST DATA
    // Motels
    val expectedRDD = sparkSession.sparkContext
      .parallelize(Seq(
        List("0000004", "Majestic Big River Elegance Plaza"),
        List("0000005", "Majestic Ibiza Por Hostel")
      ))
      .map(line => Row.fromSeq(line))

    val expectedSchema = StructType(Seq(
      StructField(Constants.MOTELS_HEADER(0), StringType, nullable = true),
      StructField(Constants.MOTELS_HEADER(1), StringType, nullable = true)
    ))

    val expected = sparkSession.createDataFrame(expectedRDD, expectedSchema)

    // Save DF as parquet.
    expected
      .write
      .parquet(getOutputPath(MOTELS_DIR))

    // TEST EXECUTION
    val motels = MotelsHomeRecommendation.getMotels(sparkSession, getOutputPath(MOTELS_DIR))
    assertDataFrameEquals(expected, motels)
  }

  @Test
  def shouldFindHighestPricesForHotels(): Unit = {

    // INPUT TEST DATA
    // Bids
    val bidsRDD = sparkSession.sparkContext
      .parallelize(Seq(
        List("0000005", "2016-08-17 15:00", "US", 1.194),
        List("0000004", "2016-08-04 15:00", "CA", 0.867),
        List("0000004", "2016-08-04 15:00", "MX", 1.317),
        List("0000005", "2016-08-17 15:00", "MX", 1.194)
      ))
      .map(line => Row.fromSeq(line))

    val bidsSchema = StructType(Seq(
      StructField(Constants.BIDS_HEADER(0),   StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(1),   StringType, nullable = true),
      StructField(Constants.CUSTOM_HEADER(1), StringType, nullable = false),
      StructField(Constants.CUSTOM_HEADER(2), DoubleType, nullable = true)
    ))

    val bids = sparkSession.createDataFrame(bidsRDD, bidsSchema)

    // Motels
    val motelsRDD = sparkSession.sparkContext
      .parallelize(Seq(
        List("0000004", "Majestic Big River Elegance Plaza"),
        List("0000005", "Majestic Ibiza Por Hostel")
      ))
      .map(line => Row.fromSeq(line))

    val motelsSchema = StructType(Seq(
      StructField(Constants.MOTELS_HEADER(0), StringType, nullable = true),
      StructField(Constants.MOTELS_HEADER(1), StringType, nullable = true)
    ))

    val motels = sparkSession.createDataFrame(motelsRDD, motelsSchema)

    // EXPECTED TEST DATA
    // Enriched Bids
    val expectedRDD = sparkSession.sparkContext
      .parallelize(Seq(
        List("0000004", "Majestic Big River Elegance Plaza", "2016-08-04 15:00", "MX", 1.317),
        List("0000005", "Majestic Ibiza Por Hostel", "2016-08-17 15:00", "US", 1.194),
        List("0000005", "Majestic Ibiza Por Hostel", "2016-08-17 15:00", "MX", 1.194)
      ))
      .map(line => Row.fromSeq(line))

    val expectedSchema = StructType(Seq(
      StructField(Constants.BIDS_HEADER(0),   StringType, nullable = true),
      StructField(Constants.MOTELS_HEADER(1), StringType, nullable = true),
      StructField(Constants.BIDS_HEADER(1),   StringType, nullable = true),
      StructField(Constants.CUSTOM_HEADER(1), StringType, nullable = false),
      StructField(Constants.CUSTOM_HEADER(2), DoubleType, nullable = true)
    ))

    val expected = sparkSession.createDataFrame(expectedRDD, expectedSchema)

    // TEST EXECUTION
    val enrichedBids = MotelsHomeRecommendation.getEnriched(bids, motels)
    assertDataFrameEquals(expected, enrichedBids)
  }

  @Test
  def shouldFilterErrorsAndCreateCorrectAggregates(): Unit = {

    runIntegrationTest()

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertRddTextFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  @Test(expected = classOf[SparkException])
  def shouldFailBecauseOfBidsWrongFormat(): Unit = {
      runFailingIntegrationTest()
  }

  @After
  def teardown(): Unit = {
    outputFolder.delete
  }

  private def runIntegrationTest(): Unit = {
    MotelsHomeRecommendation.processData(sparkSession, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def runFailingIntegrationTest(): Unit = {
    MotelsHomeRecommendation.processData(sparkSession, INPUT_BIDS_INTEGRATION_TXT, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String): Unit = {

    // No need to use DataFrames here, so just use existing sparkContext.
    val expected = sparkSession.sparkContext.textFile(expectedPath)
    val actual   = sparkSession.sparkContext.textFile(actualPath)

    RddComparator.printDiff(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}

object MotelsHomeRecommendationTest {

  var sparkSession: SparkSession = _

  @BeforeClass
  def beforeTests(): Unit = {
    sparkSession = SparkSession
      .builder()
      .appName("motels-home-recommendation test")
      .master("local[2]")
      .getOrCreate()
  }

  @AfterClass
  def afterTests(): Unit = {
    sparkSession.stop
  }
}
