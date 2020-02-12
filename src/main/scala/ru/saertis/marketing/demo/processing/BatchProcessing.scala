package ru.saertis.marketing.demo.processing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ru.saertis.marketing.demo.domain._
import ru.saertis.marketing.demo.functions._

object BatchProcessing extends App {
    
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    
    implicit val spark: SparkSession = SparkSession.builder
        .master("local[3]")
        .appName("Batch processing")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    
    spark.udf.register("markEndSession", markEndSession)
    
    spark.udf.register("addUUID", addUUID)
    
    val mobileCsv = readClickStreamCSV(spark, "data/mobile-app-clickstream.csv").cache()
    
    mobileCsv.createOrReplaceTempView("mobileCSV")
    
    val mobileClickStream = mapClickStreamDeclarative(spark, mobileCsv)
        .cache()
    
    mapClickStramOverCustomAggregator(spark, mobileCsv).show()
    
    val mobileClickStreamSQLDS = spark.sql(mobileClickStreamSQL).cache()
    
    mobileClickStreamSQLDS.createOrReplaceTempView("mobileClickStreamSQL")
    
    
    mobileClickStream.show(39, false)
    
    mobileClickStreamSQLDS.show()
    
    val purchases = readPurchaseCSV(spark, "data/purchases_sample.csv").cache()
    
    
    purchases.show()
    
    purchases.createOrReplaceTempView("purchase")
    
    //todo: possible repatition nedded
    val purchaseStatistic = purchases
        .join(mobileClickStream, array_contains(mobileClickStream("purchases"),purchases("purchaseId")))
        .select("purchaseId","purchaseTime","billingCost","isConfirmed","sessionId","campaignId","channelId").cache()
    
    
    println("Purchases Attribution Projection:")
    purchaseStatistic.show()
    
    val projectionSQL = spark.sql(purchaseProjection).cache()
    projectionSQL.show()
    projectionSQL.createOrReplaceTempView("purchasesView")
    
    println("Top Campaigns declarative:")
    topCampaign(purchaseStatistic)
        .show(10)
    
    println("Top Campaigns over SQL:")
    spark.sql(purchasesStatisticSQL).show(10)
    
    println("Channels engagement performance declarative:")
    performanceCampaign(mobileClickStream)
        .show()
    
    println("Channels engagement performance over SQL:")
    spark.sql(campaignStatisticSQL).show()
    
    
    
    
    spark.close()
    
}
