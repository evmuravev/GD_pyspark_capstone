
import pyspark.sql.types as T


mobile_app_clickstream_schema  = T.StructType([
    T.StructField("userId"    , T.StringType()),
    T.StructField("eventId"   , T.StringType()),
    T.StructField("eventType" , T.StringType()),
    T.StructField("eventTime" , T.TimestampType()),
    T.StructField("attributes", T.MapType(T.StringType(), T.StringType())),
])

user_purchases_schema  = T.StructType([
    T.StructField("purchaseId"  , T.StringType()),
    T.StructField("purchaseTime", T.TimestampType()),
    T.StructField("billingCost" , T.DoubleType()),
    T.StructField("isConfirmed" , T.BooleanType()),
])

purchases_attribution_schema  = T.StructType([
    T.StructField("purchaseId"  , T.StringType()),
    T.StructField("purchaseTime", T.TimestampType()),
    T.StructField("billingCost" , T.DoubleType()),
    T.StructField("isConfirmed" , T.BooleanType()),
    T.StructField("sessionId"  , T.StringType()),
    T.StructField("campaignId"  , T.StringType()),
    T.StructField("channelId"  , T.StringType()),
])

