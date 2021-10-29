
import pytest
from pyspark.sql import SparkSession
import main
import schemas as sch


@pytest.fixture(scope="session") 
def spark_session() -> SparkSession: 
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    return spark


@pytest.fixture(scope="session")
def clickstream_df(spark_session):
    schema = sch.mobile_app_clickstream_schema
    path ='test/test_dataset/mobile_app_clickstream.csv.gz'
    df = main.prepare_df(spark_session, path, schema)
    return df


@pytest.fixture(scope="session")
def user_purchases_df(spark_session):
    schema = sch.user_purchases_schema
    path ='test/test_dataset/user_purchases.csv.gz'
    df = main.prepare_df(spark_session, path, schema)
    return df
