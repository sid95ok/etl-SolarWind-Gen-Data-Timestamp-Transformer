from pathlib import Path
import pandas as pd
from commons.logging import logger


def write_solar_data(solar_df: pd.DataFrame, date_range: list) -> bool:
    """
    Writes finally transformed solar data in storage, doesn't return anything
    :param solar_df: transformed solar generation data dataframe
    :param date_range: Range of dates for which the data has been processed
    :return: None
    """
    result = False
    count_flag = 0
    try:
        folder_name = date_range[0]
        base_dir = f"generation_output/solar/{folder_name}/"
        output_dir = Path(base_dir)
        output_dir.mkdir(parents=True, exist_ok=True)  # To prevent non-existing directory error
        # Writing date wise data into separate files
        for date_value in date_range:
            path = base_dir + date_value + ".json"
            date_df = solar_df[solar_df["utc_timestamp"].str.contains(date_value)]
            if not date_df.empty:
                date_df.to_json(path, orient='records', lines=True)
                count_flag = 1
        if count_flag:
            result = True
    except Exception as e:
        logger.error("Error while writing solar data: Please contact dev team")
        logger.error(e, exc_info=True)
    return result


def write_wind_data(wind_df: pd.DataFrame, date_range: list) -> bool:
    """
    Writes finally transformed wind data in storage, doesn't return anything
    :param wind_df: transformed wind dataframe
    :param date_range: Range of dates for which the data has been processed
    :return: None
    """
    result = False
    count_flag = 0
    try:
        folder_name = date_range[0]
        base_dir = f"generation_output/wind/{folder_name}/"
        output_dir = Path(base_dir)
        output_dir.mkdir(parents=True, exist_ok=True)  # To prevent non-existing directory error
        # Writing date wise data into separate files
        for date_value in date_range:
            path = base_dir + date_value + ".csv"
            date_df = wind_df[wind_df["utc_timestamp"].str.contains(date_value)]
            if not date_df.empty:
                count_flag = 1
                date_df.to_csv(path, index=False)
        if count_flag:
            result = True
    except Exception as e:
        logger.error("Error while writing wind data: Please contact dev team")
        logger.error(e, exc_info=True)
    return result
