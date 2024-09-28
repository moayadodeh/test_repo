from dar.helpers.log_config import setup_logging
from dar.scripts.extract_from_postgres.extract_from_postgres import ExtractFromPostgres
import logging
def setup_environment():
    setup_logging()

def main():
    ExtractFromPostgres().extract(start_date="10/7/2021")#,end_date="12/7/2021")

if __name__ == '__main__':
    setup_environment()
    logging.info('Start Script')
    main()
