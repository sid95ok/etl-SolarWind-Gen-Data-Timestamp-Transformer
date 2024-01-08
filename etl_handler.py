from commons.logging import logger


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
        pass

    except Exception as e:
        logger.error("Error in ETL pipeline: Please contact dev team")
        logger.error(e, exc_info=True)


if __name__ == "__main__":
    etl_handler()
