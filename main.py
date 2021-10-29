from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
import sys

import schemas as sch


#Applying schema with json-like attributes converted to theMapType
def apply_schema(df: DataFrame, schema: T.StructType) -> DataFrame:
    for field in schema: 
        if type(field.dataType) is T.MapType:
            df = df.withColumn(field.name, 
                    F.from_json(F.col(field.name), T.MapType(T.StringType(), T.StringType()))
                )
        else: df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))
    return df


def prepare_df(spark: SparkSession, path, schema):
    df = spark.read.csv(path, header=True)
    df = apply_schema(df, schema)
    return df


# Task 1.1.
def build_purchases_attribution_sql(spark: SparkSession, user_purchases_df: DataFrame, clickstream_df: DataFrame) -> DataFrame:
    user_purchases_df.createOrReplaceTempView('user_purchases')
    clickstream_df.createOrReplaceTempView('clickstream')
    return spark.sql('''
        WITH cs_with_session AS (
            SELECT 
                eventTime,
                attributes.campaign_id as campaignId, 
                attributes.channel_id  as channelId,
                attributes.purchase_id as purchaseId,
                SUM(CASE WHEN eventType = 'app_open' THEN 1 ELSE 0 END) 
                    OVER (ORDER BY userId, eventTime) AS sessionId
            FROM clickstream
        ), 
        purchase_attr_from_cs AS (
            SELECT 
                purchaseId,
                sessionId,
                FIRST_VALUE(campaignId,TRUE) over (partition by sessionId order BY eventTime) AS campaignId,
                FIRST_VALUE(channelId,TRUE) over (partition by sessionId order BY eventTime) AS channelId
            FROM cs_with_session 
        )
        SELECT 
            t1.purchaseId,
            t2.purchaseTime,
            t2.billingCost,
            t2.isConfirmed,
            STRING(t1.sessionId),
            t1.campaignId,
            t1.channelId
        FROM purchase_attr_from_cs t1
        LEFT JOIN user_purchases t2
            ON t1.purchaseId = t2.purchaseId
        WHERE t1.purchaseId IS NOT NULL
    ''')


# Task 1.2.
def is_mathced(str, value):
    return 1 if str == value else 0

is_mathcedUDF = F.udf(is_mathced, T.IntegerType())


def build_purchases_attribution_df(user_purchases_df: DataFrame, clickstream_df: DataFrame) -> DataFrame:
    attributes = {
        'campaign_id':'campaignId', 
        'channel_id':'channelId',
        'purchase_id':'purchaseId'
    }
    attributes = [F.col("attributes").getItem(k).alias(v) for k,v in attributes.items()]
    prop_window = Window.partitionBy('sessionId').orderBy('eventTime')
    clickstream_df =  (   
        clickstream_df.withColumn('sessionId',
                        F.sum(is_mathcedUDF('eventType',F.lit('app_open')))
                        .over(Window.orderBy(['userId', 'eventTime']))
                      )
                      .select(['*'] + attributes)
                      .withColumn('campaignId', F.first('campaignId', True).over(prop_window))
                      .withColumn('channelId', F.first('channelId', True).over(prop_window))
                      .select(['purchaseId','sessionId','campaignId','channelId'])
                      .where(F.col('purchaseId').isNotNull())
    )
    user_purchases_df = user_purchases_df.select(['purchaseId','purchaseTime','billingCost','isConfirmed'])
    result = (clickstream_df.join(user_purchases_df, on='purchaseId', how='left')
                            .select(['purchaseId','purchaseTime','billingCost','isConfirmed',
                                     F.col('sessionId').cast(T.StringType()),'campaignId','channelId']))
    return result


def main(spark: SparkSession, clickstream_path, user_purchases_path, out_path=None):

    clickstream_df = prepare_df(spark, clickstream_path, sch.mobile_app_clickstream_schema)
    user_purchases_df = prepare_df(spark, user_purchases_path, sch.user_purchases_schema)
    purchases_attribution_df = build_purchases_attribution_sql(spark, user_purchases_df, clickstream_df).cache()
    purchases_attribution_df.createOrReplaceTempView('purchases_attribution')
    purchases_attribution_df.write.parquet('results/purchases_attribution.parquet', mode='overwrite')

    # Task 2.1.
    top_ten_campaign = spark.sql('''
        SELECT 
            campaignId, 
            ROUND(SUM(billingCost),2) as revenue
        FROM purchases_attribution  
        WHERE isConfirmed=TRUE
        GROUP BY campaignId
        ORDER BY revenue DESC
        LIMIT 10
    ''')
    top_ten_campaign.write.parquet('results/top_ten_campaign.parquet', mode='overwrite')

    # Task 2.2.
    top_channels_in_campaign = spark.sql('''
        SELECT 
            channelId, 
            campaignId,
            engagements
        FROM ( 
            SELECT 
                channelId, 
                campaignId, 
                COUNT(sessionId) as engagements,
                ROW_NUMBER() OVER(PARTITION BY campaignId ORDER BY COUNT(sessionId) DESC) as rn
            FROM purchases_attribution  
            GROUP BY channelId, campaignId
            ORDER BY engagements DESC) T
        WHERE rn=1
    ''')
    top_channels_in_campaign.write.parquet('results/top_channels_in_campaign.parquet', mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    clickstream_path = sys.argv[1] + '/*'
    user_purchases_path = sys.argv[2] + '/*'

    main(spark, clickstream_path, user_purchases_path)

