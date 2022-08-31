from sys import argv
from pyspark.sql import SparkSession

if __name__ == "__main__":
    filepath1, filepath2, countries = argv[1:4]
    countries = countries[1:-1].split(',')
    print(filepath1, filepath2, countries)

    spark = SparkSession.builder.appName("codac").getOrCreate()
    sc = spark.sparkContext
    client_df = spark.read.option("header", True).option("inferSchema", True).csv(filepath1)
    finance_df = spark.read.option("header", True).option("inferSchema", True).csv(filepath2)
    client_df.show()
    finance_df.show()