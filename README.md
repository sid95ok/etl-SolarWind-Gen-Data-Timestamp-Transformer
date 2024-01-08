# ETL-SolarWind-Data-Timestamp-Transformer
ETL client that will extract Solar and Wind data, transform naive timestamps to UTC, and finally load the data to a directory.

###### Preferred Python version >= 3.9 (https://devguide.python.org/versions/)

#### This ETL client will process in below steps:
1. Extract data from latest week from both: `Solar` and `Wind` endpoints.
2. Transform naive timestamps from the data source to a timezone aware `utc` format.
3. Load the data to an `/generation_output` directory, using `JSON` for `Solar` and `CSV` for `Wind` data.


### Getting Started

1. Setup a virtual or package manager environment (`pipenv`, `conda`, etc.)
2. Install requirements into your environment: `pip install -r requirements.txt`
3. Run the pipeline using this command data source: `API_KEY='<api_key>' API_SERVER_URL='<server_url>' python etl_handler.py`
#### Note: Replace api_key and server_url with the proper values.
4. If needed, ETL pipeline can be run for some other date as well instead of today's date by adding one
more env variable `LATEST_DATE` with format `YYYY-MM-DD` and then completed command would be
`API_KEY='<api_key>' API_SERVER_URL='<server_url>' LATEST_DATE='YYYY-MM-DD' python etl_handler.py`
#### Note: API_KEY used here is `NOT SAFE`, it is used just for the purpose of testing.
#### While actual deployment it should be placed safely somewhere like AWS Secretsmanager, Parameters, etc. as per the requirements.


### Tests (Unit and Integration)
1. To run all the tests: `API_KEY='<api_key>' API_SERVER_URL='<server_url>' python -m coverage run -m unittest`
#### Note: Replace api_key and server_url with the proper values
2. To see the final coverage report: `python -m coverage report`
