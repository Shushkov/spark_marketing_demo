## BigData Tasks: Marketing Analytics
### Motivation
There is a mobile application that is used by customers to transact with its on-line store. Marketing department of the company has set up various campaigns (e.g. “Buy one thing and get another one as a gift”, etc.)  via different marketing channels (e.g. Google / Yandex / Facebook Ads, etc.).
Now the business wants to know the efficiency of the campaigns and channels.
Let’s help them out!

### Given datasets
- Mobile App clickstream projection
```
userId: String
eventId: String
eventTime: Timestamp
eventType: String
attributes: Option[Map[String, String]]
```
   - event types:
```
app_open
 search_product
 view_product_details
 purchase 
 app_close

```

- Purchases projection
```
purchaseId: String
purchaseTime: Timestamp
billingCost: Double
isConfirmed: Boolean
```

---
Datasets located in the project's data directory 
### Task #1:

- Build Purchases Attribution Projection
The projection is dedicated to enabling a subsequent analysis of marketing campaigns and channels. 

The target schema:

```
purchaseId: String,
purchaseTime: Timestamp,
billingCost: Double,
isConfirmed: Boolean,

// a session starts with app_open event and finishes with app_close 
sessionId: String,
campaignId: String,  // derived from app_open#attributes#campaign_id
channelIid: String,    // derived from app_open#attributes#channel_id
```

### Task #2:
- Calculate Marketing Campaigns And Channels Statistics 
   - Top Campaigns:  What are the Top 10 marketing campaigns that bring the biggest revenue (based on billingCost of confirmed purchases)?
   - Channels engagement performance: What is the most popular (i.e. Top) channel that drives the highest amount of unique sessions (engagements)  with the App in each campaign?

#### Build
From project directory run following command:

`sbt package`
#### Run Demo
From project directory run following command:

```spark-submit --class "ru.saertis.marketing.demo.processing.BatchProcessing" --master "local[*]" target/scala-2.11/marketing_analytics_demo_2.11-0.1.jar```
