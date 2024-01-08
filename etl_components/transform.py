import pandas as pd
from commons.logging import logger
from typing import Union
from commons.util import (
    convert_naive_timestamp_to_utc_datetime, remove_str_whitespaces,
    convert_utc_datetime_to_naive_datetime
)


def transform_column_names(df: pd.DataFrame) -> Union[pd.DataFrame, None]:
    """
    Takes a pandas dataframe as an input and corrects its columns names if
        there are any anomalies and convert them to snake_case
    :param df: Pandas Dataframe
    :return: transformed pandas df with corrected column names
    """
    if not isinstance(df, pd.DataFrame):
        return
    if df.empty:
        return df
    df.rename(columns=lambda col_name: remove_str_whitespaces(col_name), inplace=True)
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ', '_')
    return df


def transform_solar_data(solar_df: pd.DataFrame) -> Union[pd.DataFrame, None]:
    """
    Transform solar data and convert naive timestamps to UTC
    :param solar_df: solar generation data dataframe
    :return: transformed solar dataframe
    """
    if not isinstance(solar_df, pd.DataFrame):
        return
    if solar_df.empty:
        return solar_df
    try:
        solar_df["utc_timestamp"] = solar_df.apply(
            lambda row: convert_naive_timestamp_to_utc_datetime(
                row["naive_timestamp"]), axis=1)
        return solar_df
    except Exception as e:
        logger.error("Error while transforming solar data: Please contact dev team")
        logger.error(e, exc_info=True)


def transform_wind_data(wind_df: pd.DataFrame) -> Union[pd.DataFrame, None]:
    """
    Transform wind data and convert naive timestamps to UTC
    :param wind_df: wind generation data dataframe
    :return: transformed wind dataframe
    """
    if not isinstance(wind_df, pd.DataFrame):
        return
    if wind_df.empty:
        return wind_df
    try:
        wind_df["utc_timestamp"] = wind_df["naive_timestamp"]
        wind_df["naive_timestamp"] = wind_df.apply(
            lambda row: convert_utc_datetime_to_naive_datetime(
                row["naive_timestamp"]), axis=1)
        return wind_df
    except Exception as e:
        logger.error("Error while transforming wind data: Please contact dev team")
        logger.error(e, exc_info=True)
