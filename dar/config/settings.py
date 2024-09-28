from typing import Dict, Union
from dotenv import load_dotenv
import os

load_dotenv()


MAIN_DIR = os.getenv('MAIN_DIR')

CODE_DIR = MAIN_DIR + 'src/'
DATA_DIR = MAIN_DIR + 'data/'
SQL_DIR = CODE_DIR + 'sql/'

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')


def get_settings() -> Dict[str, Union[str,int, Dict[str, str]]]:
    """Get the application settings as a dictionary.

    Returns:
        Dict[str, Union[str, Dict[str, str]]]: The application settings.
    """
    settings = {
        name.lower(): 
        value if not isinstance(value, dict) else value
        for name, value in globals().items()
        if name.isupper() and not name.startswith("__")
    }
    return settings

