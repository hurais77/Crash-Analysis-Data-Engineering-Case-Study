import logging
logger = logging.getLogger(__name__)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)s] - %(message)s'))
logger.addHandler(stream_handler)
logger.setLevel(logging.DEBUG)