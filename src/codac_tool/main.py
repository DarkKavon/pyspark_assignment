import os
import logging
import argparse
from logging.handlers import RotatingFileHandler
import datetime as dt
from typing import Union
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

log_filepath = "kommatipara.log"


def create_rotating_log(path: str, size: int = 4096) -> logging.Logger:
    """
    Creates a logger for rotating logs.
    :param str path: path to rotating log file
    :return: rotating logger
    :rtype: logging.Logger
    """
    logger = logging.getLogger("rotating_logger")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(path, maxBytes=size, backupCount=5)
    formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def read_file(spark: SparkSession, filepath: str) -> DataFrame:
    """
    Reads comma-separated dataset into Spark dataframe.
    :param SparkSession spark: existing Spark session
    :param str filepath: path to dataset
    :return: Spark dataframe
    :rtype: DataFrame
    """
    return spark.read.option("header", True).option("inferSchema", True).csv(filepath)


def rename_columns(df: DataFrame, column_names_mapping: dict) -> DataFrame:
    """
    Renames column names using provided mapping.
    :param DataFrame df: existing dataframe for renaming columns
    :param dict column_names_mapping: dictionary containing column names mapping {"old": "new"}
    :return: dataframe with renamed columns
    :rtype: DataFrame
    """
    for k, v in column_names_mapping.items():
        df = df.withColumnRenamed(k, v)
    return df


def filter_column(df: DataFrame, column_name: str, values: Union[str, list]) -> DataFrame:
    """
    Filters dataframe to preserve given values.
    :param DataFrame df: existing dataframe for filtering
    :param str column_name: column for filter to be applied
    :param str|list values: values to preserve
    :return: filtered dataframe
    :rtype: DataFrame
    """
    if type(values) != list:
        value = values
        values = []
        values.append(value)
    return df.filter(col(column_name).isin(values))


if __name__ == "__main__":
    log = create_rotating_log(log_filepath)
    log.info("Starting new run.")

    # create arguments parser
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath1", help="Filepath to the clients' dataset")
    parser.add_argument("filepath2", help="Filepath to th financial dataset")
    parser.add_argument(
        "countries", help="List of countries to preserve, eg. [country1, country2]")
    parser.add_argument(
        "--verbose", "-v", help="Logs will be printed in terminal", action="store_true")
    args = parser.parse_args()

    # resolve if verbose
    if args.verbose:
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)-8s %(message)s")
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(formatter)
        log.addHandler(consoleHandler)

    # parse countries
    countries = [e.strip().replace("'", "").replace('"', "").replace(
        '[', "").replace(']', "") for e in args.countries.split(',')]
    log.info("Filepaths: " + args.filepath1 + " " + args.filepath2)
    log.info("Countries: " + str(countries))

    # create Spark session
    spark = SparkSession.builder.appName("codac").getOrCreate()
    sc = spark.sparkContext

    # read files
    client_df = read_file(spark, args.filepath1)
    finance_df = read_file(spark, args.filepath2)
    log.info("Files read.")

    # drop specified columns
    client_df = client_df.drop("first_name", "last_name")
    finance_df = finance_df.drop("cc_n")
    log.info("Columns dropped.")

    # join datasets by id
    df = client_df.join(finance_df, "id")
    log.info("Dataframes joined.")

    # rename columns
    df = rename_columns(df, {'id': 'client_identifier',
                        'btc_a': 'bitcoin_address', 'cc_t': 'credit_card_type'})
    log.info("Columns renamed.")

    # filter columns to preserve countries form arguments
    df = filter_column(df, "country", countries)
    log.info("Columns filtered.")

    # specify target path
    script_path = os.path.dirname(os.path.realpath(__file__))
    target_path = os.path.join(script_path, '..', '..', 'client_data', dt.datetime.strftime(
        dt.datetime.now(), "%Y%m%d%H%M%S"))

    # save file on disk
    df.coalesce(1).write.csv(target_path)
    log.info("File written.")
    log.info("Run end.")
