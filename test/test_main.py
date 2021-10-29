
import pytest
import main
import schemas as sch
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T


@pytest.mark.usefixtures("spark_session")
def test_apply_schema(spark_session):
    path1 ='test/test_dataset/user_purchases.csv.gz'
    df1 = spark_session.read.csv(path1, header=True)

    path2 ='test/test_dataset/mobile_app_clickstream.csv.gz'
    df2 = spark_session.read.csv(path2, header=True)

    assert sch.user_purchases_schema == main.apply_schema(df1, sch.user_purchases_schema).schema
    assert sch.mobile_app_clickstream_schema == main.apply_schema(df2, sch.mobile_app_clickstream_schema).schema


@pytest.mark.usefixtures('spark_session')
def test_prepare_user_purchases_df(spark_session,):
    schema = sch.user_purchases_schema
    path ='test/test_dataset/user_purchases.csv.gz'
    df = main.prepare_df(spark_session, path, schema)

    assert df.schema == schema
    assert df.count() > 0


@pytest.mark.usefixtures('spark_session')
def test_prepare_clickstream_df(spark_session):
    schema = sch.mobile_app_clickstream_schema
    path ='test/test_dataset/mobile_app_clickstream.csv.gz'
    df = main.prepare_df(spark_session, path, schema)

    assert df.schema == schema
    assert df.count() > 0


@pytest.mark.usefixtures('spark_session','user_purchases_df','clickstream_df')
def test_build_purchases_attribution_sql(spark_session, user_purchases_df, clickstream_df):
    df = main.build_purchases_attribution_sql(spark_session, user_purchases_df, clickstream_df)

    assert df.schema == sch.purchases_attribution_schema
    assert df.count() > 0


@pytest.mark.usefixtures('spark_session','user_purchases_df','clickstream_df')
def test_build_purchases_attribution_df(spark_session, user_purchases_df, clickstream_df):
    df = main.build_purchases_attribution_df(user_purchases_df, clickstream_df)

    assert df.schema == sch.purchases_attribution_schema
    assert df.count() > 0


@pytest.mark.usefixtures('spark_session','user_purchases_df','clickstream_df')
def test_purchases_attribution_equality(spark_session, user_purchases_df, clickstream_df):
    errors = []
    df1 = main.build_purchases_attribution_df(user_purchases_df, clickstream_df)
    df2 = main.build_purchases_attribution_sql(spark_session, user_purchases_df, clickstream_df)
    if df1.schema != df2.schema:
        errors.append("The schemas are  different")
    if df1.collect() != df2.collect():
        errors.append("The data in the dfs do not match")
        
    assert not errors, "errors occured:\n{}".format("\n".join(errors))



