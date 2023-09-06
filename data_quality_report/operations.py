import pyspark.sql.functions as F
from spark import active_session

spark = active_session()


def transpose_one_row_df(df, result_name):
    # method to convert a one row dataframe values to columns
    column_names = df.columns
    return spark.createDataFrame([(column_name, df.first()[i]) for i, column_name in enumerate(column_names)],
                                 ["Column", result_name])


def get_missing_df(df):
    # method to count the missing values from columns of a dataframe
    missing_data_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])
    return transpose_one_row_df(missing_data_counts, "Missing")


def get_present_df(df):
    # method to count the present values from columns of a dataframe
    present_data_counts = df.select([F.sum(F.col(c).isNotNull().cast("int")).alias(c) for c in df.columns])
    return transpose_one_row_df(present_data_counts, "Present")


def get_unique_df(df):
    # method to count the unique values from columns of a dataframe
    unique_data_counts = df.select([F.countDistinct(F.col(c)).alias(c) for c in df.columns])
    return transpose_one_row_df(unique_data_counts, "Unique")


def get_max_df(df):
    # method to calculate the max value from dataframes columns
    max_values = df.select([F.max(F.col(c)).alias(c) for c in df.columns])
    return transpose_one_row_df(max_values, "Max")


def get_min_df(df):
    # method to calculate the min value from dataframes columns
    min_values = df.select([F.min(F.col(c)).alias(c) for c in df.columns])
    return transpose_one_row_df(min_values, "Min")
