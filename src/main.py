from sys import argv
from pyspark.sql import SparkSession

if __name__ == "__main__":
    file1, file2, countries = argv[1:4]
    countries = countries[1:-1].split(',')
    print(file1, file2, countries)

    spark = SparkSession.builder.appName("codac").getOrCreate()
    sc = spark.sparkContext
    df = spark.read.option("header", True).option("inferSchema", True).csv(r"C:\Users\kwandel\Documents\POC\python_assignment\dataset_one.csv")
    df.show()