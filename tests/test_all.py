import logging
from src.codac_tool import main
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
import pytest
import os
import sys
sys.path.append(os.getcwd())


@pytest.fixture
def spark():
    spark_session = SparkSession.builder.appName("tests").getOrCreate()
    return spark_session


@pytest.fixture()
def schema():
    return StructType([
        StructField("id", IntegerType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True)
    ])


@pytest.fixture()
def schema2():
    return StructType([
        StructField("id", IntegerType(), nullable=True),
        StructField("fname", StringType(), nullable=True),
        StructField("lname", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True)
    ])


@pytest.fixture
def data():
    return [
        (1, "Neel", "Goady", "ngoady0@netvibes.com"),
        (2, "Martica", "Fleg", "mfleg1@pcworld.com"),
        (3, "Waiter", "Hunter", "whunter2@hao123.com"),
        (4, "Peterus", "Goady", "pgoady3@apache.org"),
        (5, "Ase", "Ritelli", "aritelli4@seattletimes.com"),
        (6, "Francisca", "Havvock", "fhavvock5@mlb.com"),
        (7, "Piotr", "Crinion", "pcrinion6@pcworld.com"),
        (8, "Cyndi", "Durrett", "cdurrett7@comcast.net"),
        (9, "Neel", "Tonkes", "ntonkes8@shop-pro.jp"),
        (10, "Hanny", "Instrell", "hinstrell9@typepad.com")
    ]


@pytest.fixture
def df(spark, data, schema):
    return spark.createDataFrame(data=data, schema=schema)


def test_filter_column_list(spark, df, schema):
    expected_result = [
        (1, "Neel", "Goady", "ngoady0@netvibes.com"),
        (4, "Peterus", "Goady", "pgoady3@apache.org"),
        (8, "Cyndi", "Durrett", "cdurrett7@comcast.net")
    ]
    excepted_df = spark.createDataFrame(data=expected_result, schema=schema)
    filter_value = ["Goady", "Durrett"]
    filtered_df = main.filter_column(df, "last_name", filter_value)
    assert_df_equality(excepted_df, filtered_df)


def test_filter_column_no_list(spark, df, schema):
    expected_result = [
        (1, "Neel", "Goady", "ngoady0@netvibes.com"),
        (4, "Peterus", "Goady", "pgoady3@apache.org")
    ]
    excepted_df = spark.createDataFrame(data=expected_result, schema=schema)
    filter_value = "Goady"
    filtered_df = main.filter_column(df, "last_name", filter_value)
    assert_df_equality(excepted_df, filtered_df)


def test_rename_columns(spark, df, data, schema2):
    excepted_df = spark.createDataFrame(data=data, schema=schema2)
    renamed_df = main.rename_columns(
        df, {"first_name": "fname", "last_name": "lname"})
    assert_df_equality(excepted_df, renamed_df)


def test_rename_columns_skip(spark, df, data, schema2):
    excepted_df = spark.createDataFrame(data=data, schema=schema2)
    renamed_df = main.rename_columns(
        df, {"first_name": "fname", "last_name": "lname", "too_skip": "skip"})
    assert_df_equality(excepted_df, renamed_df)


def test_read_file(spark, df):
    read_df = main.read_file(spark, os.getcwd()+"/tests/dataset_test.csv")
    assert_df_equality(df, read_df)


def test_create_rotating_log():
    logger = main.create_rotating_log("testlogger.log")
    os.remove("testlogger.log")
    assert logging.Logger == type(logger)
