import os
import logging
from logging.handlers import RotatingFileHandler
import datetime as dt
from sys import argv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

log_filepath = "kommatipara.log"


def create_rotating_log(path, size=4096):
    logger = logging.getLogger("rotating_logger")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(path, maxBytes=size, backupCount=5)
    foramtter = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
    handler.setFormatter(foramtter)
    logger.addHandler(handler)
    return logger


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
    log = create_rotating_log(log_filepath)
    log.info("Starting new run.")
    if len(argv) != 4:
        print('Too few or too many arguments!\nReminder: python src/main.py "filepath1" "filepath2" "[list,of,expected,countries]"')
        log.info("Too few or too many arguments.")
        exit(1)
    filepath1, filepath2, countries = argv[1:4]
    countries = [e.strip() for e in countries[1:-1].split(',')]
    log.info("Filepaths:", filepath1, filepath2)
    log.info("Countries:", countries)

    spark = SparkSession.builder.appName("codac").getOrCreate()
    sc = spark.sparkContext
    client_df = read_file(filepath1)
    finance_df = read_file(filepath2)
    log.info("Files read.")
    
    client_df = client_df.drop("first_name", "last_name")
    finance_df  = finance_df.drop("cc_n")
    log.info("Columns dropped.")

    df = client_df.join(finance_df, "id")
    log.info("Dataframes joined.")

    df = rename_columns(df, {'id': 'client_identifier', 'btc_a': 'bitcoin_address', 'cc_t': 'credit_card_type'})
    log.info("Columns renamed.")

    df = filter_columns(df, "country", countries)
    log.info("Columns filtered.")

    script_path = os.path.dirname(os.path.realpath(__file__))
    target_path = os.path.join(script_path, '..', 'client_data',dt.datetime.strftime(dt.datetime.now(), "%Y%m%d%H%M%S"))

    df.coalesce(1).write.csv(target_path)
    log.info("File written.")
    log.info("Run end.")