__author__ = "reed@reedjones.me"

import asyncio
import logging
from logging.handlers import RotatingFileHandler
from datasystem import process_items, yield_json_paths, process_item
from concurrent.futures import ThreadPoolExecutor

import json
logging.basicConfig(
    level=logging.INFO,
    format="[TASKRUNNER] %(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        RotatingFileHandler("task.log", maxBytes=1000000),
        logging.StreamHandler()
    ]
)


if __name__ == '__main__':
    logging.info("Started")
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main(loop))
    # loop.close()
    process_items()


