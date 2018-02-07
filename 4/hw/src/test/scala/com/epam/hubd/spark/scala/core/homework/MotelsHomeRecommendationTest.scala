package com.epam.hubd.spark.scala.core.homework

import java.io.File
import java.nio.file.Files

import com.epam.hubd.spark.scala.core.homework.MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR}
import com.epam.hubd.spark.scala.core.homework.domain.{BidItem, EnrichedItem, MotelItem}
import com.epam.hubd.spark.scala.core.util.RddComparator
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext, SparkContextProvider}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import org.scalatest.Matchers._

/**
  * Created by Csaba_Bejan on 8/17/2016.
  */
class MotelsHomeRecommendationTest extends FunSuite with SharedSparkContext with RDDComparisons with BeforeAndAfter
  with BeforeAndAfterAll with SparkContextProvider {

  override def conf = new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test")

  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"
  val INPUT_RATES_SAMPLE = "src/test/resources/exchange_rates_sample.txt"
  val INPUT_MOTELS_SAMPLE = "src/test/resources/motels_sample.txt"

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.txt"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.txt"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/error_records"

  private var outputFolder: File = null

  before {
    outputFolder = Files.createTempDirectory("output").toFile
  }

  test("should read raw bids") {
    val expected = sc.parallelize(
      Seq(
        List("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35"),
        List("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL")
      )
    )

    val rawBids = MotelsHomeRecommendation.getRawBids(sc, INPUT_BIDS_SAMPLE)

    assertRDDEquals(expected, rawBids)
  }

  test("should collect erroneous records") {
    val rawBids = sc.parallelize(
      Seq(
        List("1", "06-05-02-2016", "ERROR_1"),
        List("2", "15-04-08-2016", "0.89"),
        List("3", "07-05-02-2016", "ERROR_2"),
        List("4", "06-05-02-2016", "ERROR_1"),
        List("5", "06-05-02-2016", "ERROR_2")
      )
    )

    val expected = sc.parallelize(
      Seq(
        "06-05-02-2016,ERROR_1,2",
        "06-05-02-2016,ERROR_2,1",
        "07-05-02-2016,ERROR_2,1"
      )
    )

    val erroneousRecords = MotelsHomeRecommendation.getErroneousRecords(rawBids)

    assertRDDEquals(expected, erroneousRecords)
  }

  test("should extract exchange rates") {
    val expected = Map[String, Double](
        "11-06-05-2016" -> 0.803,
        "11-05-08-2016" -> 0.873,
        "10-06-11-2015" -> 0.987
    )

    val rates = MotelsHomeRecommendation.getExchangeRates(sc, INPUT_RATES_SAMPLE)

    rates should be (expected)
  }

  // Use sc.parallelize with implicit data definitions OR use MotelsHomeRecommendation methods instead?
  test("should filter and transform bids to BidItems") {
    val bids = sc.parallelize(
      Seq(
        List("0000004", "15-04-08-2016", "", "1.62", "0.70", "", "1.64", "0.62", "1.08", "", "", "0.70", "", "0.57", "0.61", "0.92", "1.87"),
        List("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL"),
        List("0000005", "15-17-08-2016", "0.45", "", "0.35", "0.42", "1.21", "1.39", "", "0.44", "1.92", "1.21", "1.54", "1.58", "0.90", "1.22", "0.72", "1.53"),
        List("0000003", "23-04-02-2016", "ERROR_BID_SERVICE_TIMEOUT")
      )
    )

    val rates = Map[String, Double](
      "15-04-08-2016" -> 0.803,
      "15-17-08-2016" -> 0.987
    )

    val expected = sc.parallelize(
      Seq(
        BidItem("0000004", "2016-08-04 15:00", "CA", 0.867),
        BidItem("0000004", "2016-08-04 15:00", "MX", 1.317),
        BidItem("0000005", "2016-08-17 15:00", "US", 0.415),
        BidItem("0000005", "2016-08-17 15:00", "MX", 1.194)
      )
    )

    val bidItems = MotelsHomeRecommendation.getBids(bids, rates)

    assertRDDEquals(expected, bidItems)
  }

  test("should read motels info") {
    val expected = sc.parallelize(
      Seq(
        MotelItem("0000001", "Olinda Windsor Inn"),
        MotelItem("0000002", "Merlin Por Motel")
      )
    )

    val motels = MotelsHomeRecommendation.getMotels(sc, INPUT_MOTELS_SAMPLE)

    assertRDDEquals(expected, motels)
  }

  // Use sc.parallelize with implicit data definitions OR use MotelsHomeRecommendation methods instead?
  test("should find highest price per hotel") {
    val bidItems = sc.parallelize(
      Seq(
        BidItem("0000004", "2016-08-04 15:00", "CA", 0.867),
        BidItem("0000004", "2016-08-04 15:00", "MX", 1.317),
        BidItem("0000005", "2016-08-17 15:00", "US", 0.415),
        BidItem("0000005", "2016-08-17 15:00", "MX", 1.194)
      )
    )

    val motels = sc.parallelize(
      Seq(
        MotelItem("0000004", "Majestic Big River Elegance Plaza"),
        MotelItem("0000005", "Majestic Ibiza Por Hostel")
      )
    )

    val expected = sc.parallelize(
      Seq(
        EnrichedItem("0000004", "Majestic Big River Elegance Plaza", "2016-08-04 15:00", "MX", 1.317),
        EnrichedItem("0000005", "Majestic Ibiza Por Hostel", "2016-08-17 15:00", "MX", 1.194)
      )
    )

    val enriched = MotelsHomeRecommendation.getEnriched(bidItems, motels)

    assertRDDEquals(expected, enriched)
  }


  test("should filter errors and create correct aggregates") {

    runIntegrationTest()

    //If the test fails and you are interested in what are the differences in the RDDs uncomment the corresponding line
    printRddDifferences(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    printRddDifferences(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertAggregatedFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  after {
    outputFolder.delete
  }



  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(sc, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    assertRDDEquals(expected, actual)
  }

  private def assertAggregatedFiles(expectedPath: String, actualPath: String) = {
    val expected = parseLastDouble(sc.textFile(expectedPath)).collect.toMap
    val actual = parseLastDouble(sc.textFile(actualPath)).collect.toMap
    if (expected.size != actual.size) {
      Assert.fail(s"Aggregated have wrong number of records (${actual.size} instead of ${expected.size})")
    }
    expected.foreach(x => {
      val key = x._1
      val expectedValue = x._2
      if (!actual.contains(key)) {
        Assert.fail(s"Aggregated does not contain: $key,$expectedValue")
      }
      val actualValue = actual(key)
      if (Math.abs(expectedValue - actualValue) > 0.0011) {
        Assert.fail(s"Aggregated have different value for: $key ($actualValue instead of $expectedValue)")
      }
    })
  }
  
  private def parseLastDouble(rdd: RDD[String]) = {
    rdd.map(s => {
      val commaIndex = s.lastIndexOf(",")
      (s.substring(0, commaIndex), s.substring(commaIndex + 1).toDouble)
    })
  }

  private def printRddDifferences(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}
