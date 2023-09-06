from pyspark.sql import SparkSession
from functools import lru_cache


@lru_cache(maxsize=None)
def get_spark(app_name):
    # creates the basic spark session
    return (SparkSession.builder
            .master("local")
            .appName(app_name)
            .getOrCreate())


def active_session():
    # get the active spark session of the module
    return SparkSession.getActiveSession()
