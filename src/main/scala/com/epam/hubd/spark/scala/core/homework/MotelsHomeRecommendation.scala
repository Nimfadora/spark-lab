package com.epam.hubd.spark.scala.core.homework

import com.epam.hubd.spark.scala.core.homework.domain.{BidError, BidItem, EnrichedItem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"
  val ERRONEOUS_STRING: String = "ERROR_"

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

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
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

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    sc.textFile(bidsPath, 3).map(s => s.split(Constants.DELIMITER).toList)
  }

  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = {
    rawBids.filter(r => r(2)
      .startsWith(ERRONEOUS_STRING))
      .map(r => (BidError(r(1), r(2)), 1))
      .reduceByKey(_+_)
      .map(t => t._1.toString + Constants.DELIMITER + t._2)
  }

  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = {
    sc.textFile(exchangeRatesPath, 2).map(s => s.split(Constants.DELIMITER))
      .map(arr => (arr(0), arr(3).toDouble))
      .collect()
      .toMap
  }

  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {
    rawBids.filter(s => !s(2).startsWith(ERRONEOUS_STRING)).flatMap(data => parseBid(data, exchangeRates))
  }

  implicit class OpsNum(val str: String) extends AnyVal {
    def isNumeric: Boolean = scala.util.Try(str.toDouble).isSuccess
  }

  def exchangeMoney(bidPrice: Double, exchangeRate: Double): Double = {
    BigDecimal(bidPrice * exchangeRate).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def parseBid(data: List[String], exchangeRates: Map[String, Double]): List[BidItem] = {
    val date = data(Constants.BIDS_HEADER.indexOf("BidDate"))
    val outputDate = Constants.OUTPUT_DATE_FORMAT.print(Constants.INPUT_DATE_FORMAT.parseDateTime(date))
    val id = data(Constants.BIDS_HEADER.indexOf("MotelID"))
    val exchangeRate: Double = exchangeRates.getOrElse(date, 0)

    Constants.TARGET_LOSAS.map(losa => (losa, data(Constants.BIDS_HEADER.indexOf(losa))))
      .filter(id2price => id2price._2.isNumeric)
      .map(id2price => BidItem(id, outputDate, id2price._1, exchangeMoney(id2price._2.toDouble, exchangeRate)))
      .toList
  }

  def getMotels(sc:SparkContext, motelsPath: String): RDD[(String, String)] = {
    sc.textFile(motelsPath, 3)
      .map(s => s.split(Constants.DELIMITER))
      .map(arr => (arr(Constants.MOTELS_HEADER.indexOf("MotelID")), arr(Constants.MOTELS_HEADER.indexOf("MotelName"))))
  }

  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {
    bids.map(b => (b.motelId, b))
      .join(motels)
      .map {
        case (id, (bid, motelName)) => ((id, bid.bidDate), EnrichedItem(id, motelName, bid.bidDate, bid.loSa, bid.price))
      }
      .reduceByKey((a, b) => if(a.price >= b.price) a else b)
      .map(loSa2enriched => loSa2enriched._2)
  }


}
