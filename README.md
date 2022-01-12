# Repository with a Capstone project of the Capstone Project (Python Engineer to BigData Engineer)


This repository contains the following files:

```
├── result                                  <- the results saved as parquet files
│   ├── purchases_attribution.parquet           
│   ├── top_channels_in_campaign.parquet        
│   └── top_ten_campaign.parquet                
│
├── test                                    
│   ├── test_dataset/...                    <- folder with data for unit tests
│   └── test_main.py                        <- unit tests
│ 
├── conftest.py                             <- fixtures for unit tests
├── main.py                                 <- main script for the spark driver
├── schemas.py                              <- target schemas for spark dataframes
└── README.md
```


## Usage

Clone the repository to your local machine. cd to the directory with the repository.
To get started with the project, make sure you have installed and configured PySpark and then run the following command:
```shell
spark-submit main.py $YOUR_PATH/capstone-dataset/mobile_app_clickstream $YOUR_PATH/capstone-dataset/user_purchases

```

### Notes

PySpark version 3.1.2 running in Local mode

## Tasks

### Given datasets
#### Mobile App clickstream projection
* Schema: 
  * userId: String
  * eventId: String
  * eventTime: Timestamp
  * eventType: String
  * attributes: Map[String, String]

Purchases projection
* Schema:
  * purchaseId: String
  * purchaseTime: Timestamp
  * billingCost: Double


### Tasks 1.Build Purchases Attribution Projection
The projection is dedicated to enabling a subsequent analysis of
marketing campaigns and channels.
* The target schema:
  * purchaseId: String
  * purchaseTime: Timestamp
  * billingCost: Double
  * isConfirmed: Boolean // a session starts with app_open event and finishes with app_close
  * sessionId: String
  * campaignId: String // derived from app_open#attributes#campaign_id
  * channelIid: String // derived from app_open#attributes#channel_id
  
#### Requirements for implementation of the projection building logic:
* Task #1.1. Implement it by utilizing default Spark
SQL capabilities.
* Task #1.2. Implement it by using a custom UDF.

### Tasks 2.Calculate Marketing Campaigns And Channels Statistics

Calculate Marketing Campaigns And Channels Statistics
Use the purchases-attribution projection to build aggregates that
provide the following insights:
* Task #2.1.Top Campaigns:
  * What are the Top 10 marketing campaigns that bring the
  biggest revenue (based on billingCost of confirmed
  purchases)?
* Task #2.2.Channels engagement performance:
  * What is the most popular (i.e. Top) channel that drives the
  highest amount of unique sessions (engagements) with
  the App in each campaign?

#### Requirements for task #2:
* Should be implemented by using plain SQL on top of Spark
DataFrame API

