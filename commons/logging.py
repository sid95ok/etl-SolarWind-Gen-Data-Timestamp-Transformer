# Basic logging module for the project, might be upgraded in future according to requirements
import logging


logger = logging.getLogger('ETL Logger')
logger.setLevel(logging.INFO)
logging.basicConfig(
    level=logging.INFO,
    filemode='w',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
