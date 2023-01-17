import os
import logging
from from_root import from_root
from datetime import datetime

LOG_FILE_NAME = f"{datetime.now().strftime('%m%d%Y_%M%H%S')}.log"

LOG_DIR = 'logs'

os.makedirs(LOG_DIR,exist_ok=True)

LOG_FILE_PATH = os.path.join(LOG_DIR,LOG_FILE_NAME)

logging.basicConfig(
    filemode='w',
    filename= LOG_FILE_PATH,
    format="[%(asctime)s] %(lineno)d - %(levelname)s %(messages)s",
    level=logging.INFO
)