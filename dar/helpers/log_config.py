import logging

def setup_logging():
    logger = logging.getLogger()

    while logger.hasHandlers():
        logger.removeHandler(logger.handlers[0])

    logger.setLevel(logging.DEBUG)  # Capture all levels of logs

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Console handler for INFO level and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler for WARNING level and above (to capture warnings as well)
    error_file_handler = logging.FileHandler('error_logs.log')
    error_file_handler.setLevel(logging.WARNING)  # Changed to WARNING
    error_file_handler.setFormatter(formatter)
    logger.addHandler(error_file_handler)
