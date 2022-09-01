import os
import sys
sys.path.append(os.getcwd())
import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession, utils, dataframe
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src import main


@pytest.fixture
def spark():
    spark_session = SparkSession.builder.appName("tests").getOrCreate()
    return spark_session


@pytest.fixture()
def schema():
    return StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("first_name", StringType(), nullable=False),
        StructField("last_name", StringType(), nullable=False),
        StructField("email", StringType(), nullable=False)
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


def test_filter_column(spark, df, schema):
    expected_result = [
        (1, "Neel", "Goady", "ngoady0@netvibes.com"),
        (4, "Peterus", "Goady", "pgoady3@apache.org"),
        (8, "Cyndi", "Durrett", "cdurrett7@comcast.net")
    ]
    excepted_df = spark.createDataFrame(data=expected_result, schema=schema)
    filter_value = ["Goady", "Durrett"]
    filtered_df = main.filter_column(df, "last_name", filter_value)
    assert_df_equality(excepted_df, filtered_df)
