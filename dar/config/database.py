import psycopg2

from dar.config.settings import get_settings

import logging
settings = get_settings()

def connect_postgres():
    """
    Create a connection with database

    Returns:
    - instance of connection
    """
    ## Connection Configuration
    
    try:
        conn = psycopg2.connect(
            host = settings['postgres_host'],
            user = settings['postgres_user'],
            password = settings['postgres_password'],
            database = settings['postgres_db'],
            port = settings['postgres_port']
        )
        logging.info('Connected successfully')
        return conn
    except Exception as e:
        logging.error('Error while tying to connect to Database \n', e)
    