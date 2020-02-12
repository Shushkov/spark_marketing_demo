package ru.saertis.marketing.demo

import java.util.UUID

import ru.saertis.marketing.demo.domain._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{asc, collect_list, concat_ws, count, desc, sum, udf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import ru.saertis.marketing.demo.processing.SessionAggregator

package object functions {

  val markEndSession: UserDefinedFunction = udf { (string: String) =>
    string match {
      case "app_open" => 1
      case _ => 0
    } }

  val addUUID: UserDefinedFunction = udf{ (x:Any) => UUID.randomUUID().toString}

  val window = Window
    .partitionBy("userId")
    .orderBy(asc("eventTime"))
    .rangeBetween(Window.unboundedPreceding, 0)

  def convertToMap(string: String): Option[Map[String, String]] ={
    string match {
      case null => None
      case a =>
        val tmp = a.replaceAll("“","\"").substring(1, a.length -1)
        implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
        Some(parse(tmp).extract[Map[String, String]])
    }
  }

  def readClickStreamCSV(spark: SparkSession, path: String): Dataset[ClickStream] ={
    import spark.implicits._
    spark
      .read
      .options(Map[String, String](
        "header" -> "true",
        "inferSchema" -> "true",
        "quote" -> "\"",
        "escape" -> "\""
      ))
      .csv("data/mobile-app-clickstream.csv")
      .map(r => ClickStream(r.getString(0), r.getString(1), r.getTimestamp(2), r.getString(3), convertToMap(r.getString(4))))
  }

  def mapClickStreamDeclarative(spark: SparkSession, dataset: Dataset[ClickStream]) ={
    import spark.implicits._
    dataset
      .withColumn("session", sum(markEndSession($"eventType")).over(window))
      .groupBy("userId", "session")
      .agg(
        concat_ws("", collect_list("attributes.campaign_id")) as "campaignId",
        concat_ws("", collect_list("attributes.channel_id")) as "channelId",
        collect_list("attributes.purchase_id") as "purchases")
      .withColumn("sessionId", addUUID($"userId"))
      .cache()
  }

  def mapClickStramOverCustomAggregator(spark: SparkSession, dataset: Dataset[ClickStream]) = {
    import spark.implicits._

    val aggSession = new SessionAggregator

    dataset.withColumn("sessionId", aggSession($"eventType").over(window))
      .groupBy("userId", "sessionId")
      .agg(
        concat_ws("", collect_list("attributes.campaign_id")) as "campaignId",
        concat_ws("", collect_list("attributes.channel_id")) as "channelId",
        collect_list("attributes.purchase_id") as "purchases")
  }

  def readPurchaseCSV(spark: SparkSession, path: String): Dataset[Purchase] ={
    import spark.implicits._
    spark
      .read
      .options(Map[String, String](
        "header" -> "true",
        "inferSchema" -> "true"
      ))
      .csv("data/purchases_sample.csv")
      .as[Purchase]
  }

  def topCampaign(frame: DataFrame)={
    frame.filter("isConfirmed = true")
      .groupBy("campaignId")
      .agg(sum("billingCost") as "sum")
      .orderBy(desc("sum"))
  }

  def performanceCampaign(frame: DataFrame) = {
    frame.groupBy("campaignId","channelId")
      .agg(count("sessionId") as "count")
      .orderBy(desc("count"))
  }
}
