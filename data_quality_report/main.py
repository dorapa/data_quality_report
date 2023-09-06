import sys
from spark import get_spark
from operations import *
import findspark


def runner(target_table, spark):
    # core part of process: read table, extract information from columns, and finally generates a pdf that could be store in a blob or bucket
    # df = spark.table(target_table)
    df = spark.read.csv("path")

    # get column names and types from dataframe
    column_data_types = df.dtypes
    names_types_df = spark.createDataFrame(column_data_types, ["Column", "Data Type"])

    # compute some stats
    missing_df = get_missing_df(df)
    present_df = get_present_df(df)
    unique_df = get_unique_df(df)
    max_df = get_max_df(df)
    min_df = get_min_df(df)

    # join all the stats
    report_df = names_types_df.join(missing_df, ["Column"], 'left'). \
        join(present_df, ["Column"], 'left'). \
        join(unique_df, ["Column"], 'left'). \
        join(max_df, ["Column"], 'left'). \
        join(min_df, ["Column"], 'left')

    report_df.show()
    # here would be a component to generate a pdf with data quality and send the file to a storage.
    # in more advance software a component to generate a document o sent data to a database


#######################################################
# Main
#######################################################

if __name__ == '__main__':
    # parse input params

    findspark.init()

    target_table = sys.argv[1]

    spark = get_spark(target_table)

    runner(target_table, spark)

    spark.stop()
