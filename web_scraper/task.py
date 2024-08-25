__author__ = "reed@reedjones.me"

import asyncio
import logging
from collections import Counter
from enum import Enum
from logging.handlers import RotatingFileHandler
import os
import timeit
import trio
import aiometer
from fake import download, upload, convert, get_tasks
import time
from datasystem import process_items

logging.basicConfig(
    level=logging.INFO,
    format="[TASKRUNNER] %(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        RotatingFileHandler("task.log", maxBytes=1000000),
        logging.StreamHandler()
    ]
)

def image_to_pdf_task():
    done = 0
    for i in range(0, 100):
        done = i
        time.sleep(1)


class MaxTasks(str, Enum):
    """
    max at once
    """
    request = 1
    download = 3
    image_from_pdf = 3
    image_to_text = 3
    upload = 10
    file = 10


class TaskType(str, Enum):
    download: str = "download"
    request: str = "request"
    upload: str = "upload"
    image_from_pdf: str = "image_from_pdf"
    image_to_text: str = "image_to_text"
    file: str = "file"

    def __str__(self):
        return self.value

SECONDS_BETWEEN_TASKS = 60 * 3


def job_count():
    return len(os.listdir("json_data"))


class TaskManager(object):

    def __init__(self):
        self.jobs = get_tasks()
        self.processes = Counter(dict.fromkeys(TaskType.__members__, 0))


    def create_new_task(self, task_type: TaskType):
        pass

    def get_running_tasks(self):
        pass

    def check_task_status(self):
        pass

    def cancel_task(self):
        pass

    def start_task(self):
        pass

    def pause_task(self):
        pass

    def resume_task(self):
        pass

    def finish_task(self):
        pass

    def report(self):
        pass

    def progress(self):
        pass

    def run(self):
        x = 1
        # get 3 random urls and create are first tasks
        # wait 3 mins between starting others
        # for tasks in tasks, check status
        # if status is on pdfs,
        # check the number to do, and the number done
        # if that number hasn't changed in 9 mins
        # cancel the task
        # check how many it did?
        # if it didn't do any or less than 3, -> trash it
        # trash = move the json to stuck folder
        # if it did more that 3, and there is content in there,
        # partial it -> zip it to a folder called {title}.partial.zip and upload
        # move json to partial folder
        #ELSE if status is finished ->
        # finish task, zip upload, remove json and add another task in 3 mins
        # ELSE if status is error ->
        #  report error, move json to error folder


if __name__ == '__main__':
    logging.info("Task started")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(process_items())
