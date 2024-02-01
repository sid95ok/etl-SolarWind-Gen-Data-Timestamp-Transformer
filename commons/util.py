""" Utility methods file, where misc methods can be defined to avoid
    code duplication
"""
from os import environ
from typing import Any, Union
from datetime import date, timedelta, datetime, timezone
import requests
from requests.adapters import HTTPAdapter, Retry
from commons.logging import logger


"""Note: API_KEY used here is not safe, it is used just for the purpose of testing.
While actual deployment it should be placed safely somewhere like 
AWS Secretsmanager, Parameters, etc. as per the requirements."""
api_key = environ.get("API_KEY")
server_url = environ.get("API_SERVER_URL")

# Declaring request session handler with retry configuration, current retries - 5
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[429])
session.mount('http://', HTTPAdapter(max_retries=retries))


def get_latest_week_date_range(latest_date: str = None) -> Union[list, None]:
    """Get range of dates for last 7 days from provided date (default current latest week)
    :param latest_date: 'YYYY-MM-DD' - if this is provided, it will be used instead of today's date
    :return: list of dates
    """
    latest_date = environ.get("LATEST_DATE", latest_date)
    if not latest_date:
        latest_date = date.today()
    else:
        if not isinstance(latest_date, str):
            logger.error(f"Error in get_latest_week_date_range: date provided("
                         f"{latest_date}) is not a string")
            return
        try:
            latest_date = datetime.strptime(latest_date, "%Y-%m-%d")
        except ValueError:
            logger.warning("Format provided for the date is wrong it should be "
                           "'YYYY-MM-DD'. Terminating Pipeline...")
            return
    return [(latest_date - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(0, 7)]


def request_api_call(api_base_url: str = "/status") -> Any:
    """
    A common util function to call all `get` APIs using requests python module
    :param api_base_url: base API URL without main server URL in prefix
    :return: API Response
    """
    if not api_key or not server_url or not api_base_url:
        logger.error(
            "Arguments are missing to call the API, please contact the dev team")
        return

    try:
        api_url = f"{server_url}{api_base_url}?api_key={api_key}"
        response = session.get(api_url)
        if response.status_code == 200:
            if response.headers.get("content-type") == "text/csv":
                response = session.get(api_url).text
            elif response.headers.get("content-type") == "application/json":
                response = session.get(api_url).json()
            else:
                logger.warning("Unknown content type is returned from the API, "
                               "might hamper data integrity")
                return
            return response
        else:
            logger.warning(f"API request is failed, request code:{response.status_code}")
            logger.info(session.get(api_url).text)
            return

    except Exception as e:
        logger.error(f"Error in calling the API - {api_base_url}")
        logger.error(e)
        return


def get_solar_data(date_value: str) -> str:
    """
    Calling `Solar` data backend API on any given date
    :param date_value: YYYY-MM-DD formatted date string
    :return: API Response
    """
    return request_api_call(
        api_base_url=f"/{date_value}/renewables/solargen.json"
    )


def get_wind_data(date_value):
    """
    Calling `Wind` data backend API on any given date
    :param date_value: YYYY-MM-DD formatted date string
    :return: API Response
    """
    return request_api_call(
        api_base_url=f"/{date_value}/renewables/windgen.csv"
    )


def convert_naive_timestamp_to_utc_datetime(timestamp: [str, int, float]) -> Union[str, None]:
    """
    This util function converts given naive timestamp format(Unix) to utc datetime string
    which will have timezone info as UTC.
    :param timestamp: Naive timestamp(Unix) string to be converted
    :return: string(utc datetime string with UTC) or None (if wrong format is provided)
    """
    if not isinstance(timestamp, (str, int, float)):
        return
    try:
        if len(str(int(timestamp))) == 13:
            timestamp = int(timestamp)/1000
        else:
            timestamp = int(timestamp)
        datetime_object = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return str(datetime_object)
    except ValueError:
        logger.error("Wrong format provided in convert_naive_timestamp_to_utc_datetime method")
        return


def convert_utc_datetime_to_naive_datetime(datetime_str: str) -> Union[str, None]:
    """
    This util function removes timezone info from the give timestamp string to
    make it naive; expected format - %Y-%m-%d %H:%M:%S%z
    :param datetime_str: Timestamp with UTC provided in its timezone info
    :return: String(naive timestamp string) or None(If wrong format is provided)
    """
    if not isinstance(datetime_str, str):
        return
    try:
        datetime_object = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S%z")
        return datetime_object.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        logger.error("Wrong format provided in convert_utc_datetime_to_naive_datetime method")


def remove_str_whitespaces(string: str) -> Union[str, None]:
    """
    This util function will remove any whitespaces like duplicate space or
    spaces in start or end of the string. e.g. - ` Apple is   sweet` will be
    converted to `Apple is sweet`.
    :param string: any given string
    :return: formatted string without whitespaces
    """
    if not isinstance(string, str):
        return
    return " ".join(string.split())


def get_date_today():
    """
    This util function will return today's date in the YYYY-MM-DD format
    :return: string, today's date
    """
    return datetime.utcnow().strftime("%Y-%m-%d")
