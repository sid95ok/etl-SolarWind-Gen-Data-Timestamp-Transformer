import pandas as pd
from io import StringIO
from typing import Union, Tuple

from pandas import DataFrame

from commons.util import (
    get_latest_week_date_range, get_solar_data, get_wind_data
)
from commons.logging import logger


def extract_generation_data(latest_date: str = None) -> Union[
        Tuple[DataFrame, DataFrame, list], Tuple[None, None, None]]:
    """
    Retrieves data for solar and wind generation for last 7 days from the date
        provide(default today's date)
    :param latest_date: use it to provide a date in format 'YYYY-MM-DD' to run
        ETL for that particular week (latest_date - 7 days), otherwise today's
        date would be considered (current latest week)
    :return: Both dataframes i.e. solar and wind with the date range for which
        the data has been extracted; or None if no data found
    """
    try:
        date_range = get_latest_week_date_range(latest_date)
        if not date_range:
            logger.error("Error: Cannot get last 7 days(week) date range")
            raise Exception
        solar_df = wind_df = pd.DataFrame()
        for date in date_range:
            solar_data = get_solar_data(date_value=date)  # This is calling the API while providing date
            wind_data = get_wind_data(date_value=date)  # This is calling the API while providing date
            if solar_data:
                solar_df = pd.concat([solar_df, pd.DataFrame(solar_data)], ignore_index=True)
            if wind_data:
                wind_df = pd.concat([wind_df, pd.read_csv(StringIO(wind_data), sep=",")], ignore_index=True)
        return solar_df, wind_df, date_range
    except Exception as e:
        logger.error("Error in extraction step: Please contact dev team")
        logger.error(e, exc_info=True)
        return None, None, None
