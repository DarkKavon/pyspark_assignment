from sys import argv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def read_file(filepath):
    return spark.read.option("header", True).option("inferSchema", True).csv(filepath)


def rename_columns(df, column_names_mapping):
    for k, v in column_names_mapping.items():
        df = df.withColumnRenamed(k,v)
    return df


def filter_columns(df, column_name, values):
    if type(values) != list:
        values = list(values)
    return df.filter(col(column_name).isin(values))


if __name__ == "__main__":
    filepath1, filepath2, countries = argv[1:4]
    countries = [e.strip() for e in countries[1:-1].split(',')]
    print(filepath1, filepath2, countries)

    spark = SparkSession.builder.appName("codac").getOrCreate()
    sc = spark.sparkContext
    client_df = read_file(filepath1)
    finance_df = read_file(filepath2)
    
    client_df = client_df.drop("first_name", "last_name")
    finance_df  = finance_df.drop("cc_n")

    df = client_df.join(finance_df, "id")

    df = rename_columns(df, {'id': 'client_identifier', 'btc_a': 'bitcoin_address', 'cc_t': 'credit_card_type'})
    df = filter_columns(df, "country", countries)

    df.show()

    # client_df.show()
    # finance_df.show()
    # client_df.select("country").distinct().show()