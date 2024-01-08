from commons.logging import logger
from etl_components.extract import extract_generation_data
from etl_components.transform import (
    transform_column_names, transform_solar_data, transform_wind_data
)
from etl_components.load import write_solar_data, write_wind_data


def etl_handler(latest_date: str = None) -> None:
    """
    Main function handler to handle ETL(client) pipeline for Solar and Wind
    generation data for the latest week.
    :param latest_date: use it to provide a date in format 'YYYY-MM-DD' to run
        ETL for that particular week (latest_date - 7 days), otherwise today's
        date would be considered (current latest week)
    :return: returns None, this is main function to handle the complete ETL pipeline,
        shall change accordingly while deploying in a particular infra service
    """
    try:
        skipped_flag = False

        logger.info("Starting ETL Pipeline")
        logger.info("Stage 1: Starting extraction step for Solar and Wind data")
        solar_df, wind_df, date_range = extract_generation_data(latest_date)
        logger.info("Stage 1: Extraction completed")

        if solar_df is not None and not solar_df.empty:
            logger.info("Stage 2A: Transformation step for Solar data")
            solar_df = transform_column_names(solar_df)
            transformed_solar_df = transform_solar_data(solar_df)
            logger.info("Stage 2A: Solar data transformation completed")

            logger.info("Stage 2B: Writing Solar data")
            is_written = write_solar_data(transformed_solar_df, date_range)
            if is_written:
                logger.info("Stage 2B: Completed writing Solar data")
            else:
                skipped_flag = True
                logger.warning("Error in writing Solar data, "
                               "please check for possible errors")

        else:
            skipped_flag = True
            logger.warning("No solar data found for provided week, "
                           "skipping stage 2 for the Solar generation")

        if wind_df is not None and not wind_df.empty:
            logger.info("Stage 3A: Transformation step for Wind data")
            wind_df = transform_column_names(wind_df)
            transformed_wind_df = transform_wind_data(wind_df)
            logger.info("Stage 3A: Wind data transformation completed")

            logger.info("Stage 3B: Writing Wind data")
            is_written = write_wind_data(transformed_wind_df, date_range)
            if is_written:
                logger.info("Stage 3B: Completed writing Wind data")
            else:
                skipped_flag = True
                logger.warning("Error in writing Wind data, "
                               "please check for possible errors")

        else:
            skipped_flag = True
            logger.warning("No wind data found for provided week, "
                           "skipping stage 3 for the Wind generation")

        if not skipped_flag:
            logger.info("Finished ETL pipeline successfully")
        else:
            logger.warning("Finished ETL pipeline but few steps might be skipped, "
                           "please check for any possible errors")

    except Exception as e:
        logger.error("Error in ETL pipeline: Please contact dev team")
        logger.error(e, exc_info=True)


if __name__ == "__main__":
    etl_handler()
