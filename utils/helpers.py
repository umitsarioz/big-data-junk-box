import pandas as pd
from pyspark.sql import DataFrame, functions as f


def check_null_values(df: DataFrame, cols: list) -> pd.DataFrame:
    """
    Function check null,na or empty string values for dataframe.

    Args:
        df (DataFrame): Raw DataFrame
        cols (list): Columns check list

    Returns:
        df (pd.DataFrame):  pd.DataFrame has information about null values of given dataframe

    Notes:
        If columns are given, checks only these columns. Otherwise, check for all columns.

    """
    if not cols:
        cols = df.columns

    df = df.select([f.count(f.when(f.col(c).contains('None') |
                                   f.col(c).contains('NULL') |
                                   (f.col(c) == '') |
                                   f.col(c).isNull() |
                                   f.isnan(c), c
                                   )).alias(c)
                    for c in cols]).toPandas()
    return df


def drop_na_all(df: DataFrame, col_name: str) -> DataFrame:
    """
    Drop a column has null,na or empty string values.

    Args:
        df (DataFrame): Raw DataFrame
        cols (list): Column name will be dropped.

    Returns:
        df (pd.DataFrame):  DataFrame
    """
    df = df.filter(~(f.col(col_name).contains('None') |
                     f.col(col_name).contains('NULL') |
                     (f.col(col_name) == '') |
                     f.col(col_name).isNull() |
                     f.isnan(col_name)))
    return df
