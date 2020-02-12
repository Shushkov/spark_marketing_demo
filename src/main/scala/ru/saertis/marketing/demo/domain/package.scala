package ru.saertis.marketing.demo

import java.sql.Timestamp

package object domain {

  case class ClickStream(userId: String, eventId: String, eventTime: Timestamp, eventType: String, attributes: Option[Map[String, String]])

  case class Purchase(purchaseId: String, purchaseTime: Timestamp, billingCost: Double, isConfirmed: Boolean)

  val mobileClickStreamSQL: String =
    """
      |select
      |   temp.userId as userId,
      |   concat_ws('', collect_list(temp.attributes.campaign_id)) as campaignId,
      |   concat_ws('', collect_list(temp.attributes.channel_id)) as channelId,
      |   collect_list(temp.attributes.purchase_id) as purchases,
      |   addUUID(temp.userId) as sessionId
      |from (
      | Select userId,
      |   attributes,
      |   sum(
      |     case when eventType = 'app_open' then 1
      |      else 0
      |     end
      |   ) over (partition by userId order by eventTime asc) as session
      |
      | FROM mobileCSV ) as temp
      |group by userId, session
      |""".stripMargin

  val purchaseProjection: String =
    """
      |SELECT purchaseId,purchaseTime,billingCost,isConfirmed,sessionId,campaignId,channelId
      |FROM purchase JOIN mobileClickStreamSQL
      |ON array_contains(mobileClickStreamSQL.purchases,purchase.purchaseId)
      |""".stripMargin

  val purchasesStatisticSQL: String =
    """select campaignId,
      | sum(billingCost) as sum
      | from purchasesView
      | where isConfirmed = true
      | group by campaignId
      | order by sum desc
      |""".stripMargin


  val campaignStatisticSQL: String =
    """
      |SELECT
      | campaignId,
      | channelId,
      | count(sessionId) as count
      |FROM mobileClickStreamSQL
      |GROUP BY campaignId, channelId
      |ORDER BY count desc
      |""".stripMargin
}

