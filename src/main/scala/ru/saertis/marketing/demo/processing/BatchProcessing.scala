package ru.saertis.marketing.demo.processing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ru.saertis.marketing.demo.domain._
import ru.saertis.marketing.demo.functions._

object BatchProcessing extends App {
    
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    
    @transient lazy val log = org.apache.log4j.LogManager.getLogger("BatchProcessing")
    
    implicit val spark: SparkSession = SparkSession.builder
        .master("local[3]")
        .appName("Batch processing")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    
    spark.udf.register("markEndSession", markEndSession)
    
    spark.udf.register("addUUID", addUUID)
    
    val clickStreamPath = Settings.clickStreamCsvPath
    
    val mobileCsv = readClickStreamCSV(spark, clickStreamPath).cache()
    
    mobileCsv.createOrReplaceTempView("mobileCSV")
    
    val mobileClickStream = mapClickStreamDeclarative(spark, mobileCsv)
        .cache()
    
    mapClickStramOverCustomAggregator(spark, mobileCsv).show()
    
    val mobileClickStreamSQLDS = spark.sql(mobileClickStreamSQL).cache()
    
    mobileClickStreamSQLDS.createOrReplaceTempView("mobileClickStreamSQL")
    
    
    mobileClickStream.show(39, false)
    
    mobileClickStreamSQLDS.show()
    
    val purchasePath = Settings.purchaseCsvPath
    
    val purchases = readPurchaseCSV(spark, purchasePath).cache()
    
    
    purchases.show()
    
    purchases.createOrReplaceTempView("purchase")
    
    //todo: possible repatition nedded
    val purchaseStatistic = purchases
        .join(mobileClickStream, array_contains(mobileClickStream("purchases"),purchases("purchaseId")))
        .select("purchaseId","purchaseTime","billingCost","isConfirmed","sessionId","campaignId","channelId").cache()
    
    
    log.info("Purchases Attribution Projection:")
    purchaseStatistic.show()
    
    val projectionSQL = spark.sql(purchaseProjection).cache()
    projectionSQL.show()
    projectionSQL.createOrReplaceTempView("purchasesView")
    
    log.info("Top Campaigns declarative:")
    topCampaign(purchaseStatistic)
        .show(10)
    
    log.info("Top Campaigns over SQL:")
    spark.sql(purchasesStatisticSQL).show(10)
    
    log.info("Channels engagement performance declarative:")
    performanceCampaign(mobileClickStream)
        .show()
    
    log.info("Channels engagement performance over SQL:")
    spark.sql(campaignStatisticSQL).show()
    
    spark.close()
    
}
