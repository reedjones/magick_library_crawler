__author__ = "reed@reedjones.me"
import aiopytesseract
import json
import logging
import os

import shutil
import tempfile
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
import itertools
import aiofiles
import asyncio
import asynctempfile
import filesystem
import requests
from PIL import Image
from pdf2image import pdfinfo_from_path, convert_from_path
from pytesseract import pytesseract

logging.basicConfig(
    level=logging.INFO,
    format="[DATA] %(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        RotatingFileHandler("datasystem.log", maxBytes=1000000),
        logging.StreamHandler()
    ]
)


def clean_title(title: str):
    title = "".join([c for c in title if c.isalpha() or c.isdigit() or c == ' '])
    return title.replace(" ", "_").strip()


class APIFailed(Exception):
    pass


class WrongContent(Exception):
    pass


class NoDownload(Exception):
    pass


def fetch_pdf(url):
    r = requests.get(url, allow_redirects=True)
    if r.status_code != 200:
        raise APIFailed('API response: {}'.format(r.status_code))
    ct = r.headers.get('content-type', None)
    if ct:
        if 'html' in ct.lower() or 'text' in ct.lower():
            # logging.warning(f"Content type is wrong on {r}")
            raise WrongContent(f'Content type is html or text for {url}')
        else:
            logging.info(f"Content type is {ct}")
    return r


def try_to_fetch(url, alt_url=None, tries=0):
    if tries >= 4:
        raise NoDownload(f"Couldn't download {url}")
    t = tries
    try:
        if tries == 1 or tries == 3:
            logging.info(f"fetching {alt_url}")
            r = fetch_pdf(alt_url)
        elif tries == 0 or tries == 2:
            logging.info(f"fetching {url}")
            r = fetch_pdf(url)
        else:
            r = fetch_pdf(alt_url)
    except WrongContent as wc:
        logging.error(wc)
        time.sleep(60 * tries)
        t += 1
        return try_to_fetch(url, alt_url=alt_url, tries=t)
    except APIFailed as af:
        logging.error(af)
        time.sleep(30 * tries)
        t += 1
        return try_to_fetch(url, alt_url=alt_url, tries=t)
    except Exception as e:
        logging.error(e)
        time.sleep(9 * tries)
        t += t
        return try_to_fetch(url, alt_url=alt_url, tries=t)
    else:
        return r


def fetch(url, alt_url):
    try:
        return try_to_fetch(url, alt_url=alt_url)
    except NoDownload as e:
        logging.error(e)


def download_pdf(url, output_path, alt_url):
    r = fetch(url, alt_url)
    if not r:
        return
    with open(output_path, 'wb') as outfile:
        outfile.write(r.content)
    return output_path

async def pdf_job_finisher(event):
    print('waiting for it ...')
    await event.wait()
    print('... got it!')

class PDFJob(object):
    def __init__(self, fs, pdf_path):
        self.fs = fs
        self.finished = asyncio.Event()


def process_pdf(url, filename, data_path, data=None, alt_url=None):
    with tempfile.TemporaryDirectory() as tempdir:
        output_path = Path(tempdir) / filename
        result = download_pdf(url, output_path, alt_url)
        if not result:
            return
        try:
            pdf_info = pdfinfo_from_path(result)
        except Exception as e:
            logging.error(e)
            return None
        total_pages = pdf_info["Pages"]
        logging.info(f"Got {total_pages} pages")
        images_from_path = convert_from_path(result, output_folder=tempdir, fmt="jpg", paths_only=True)
        success = pdf_task_2(images_from_path, data_path, tempdir, data)
    if success:
        logging.info(f"Successfully Finished {url}.....")
    else:
        logging.warning(f"Problem Finishing {url}.....")
    try:
        if os.path.isdir(tempdir):
            logging.error(f"{tempdir} is still present, remove it!")
    except Exception as e:
        logging.error(e)



def pdf_task(images_from_path, data_path, temp_dir, data):
    with filesystem.AsyncFileManager(data_path=data_path, base_dir=temp_dir, data=data) as af:
        for image_path in images_from_path:
            text = str((pytesseract.image_to_string(Image.open(image_path))))
            af.files.add_page(text)


def convert(image_path):
    return pytesseract.image_to_string(Image.open(image_path))


async def image_worker(name, queue, filesys):
    while True:
        # Get a "work item" out of the queue.
        image_path = await queue.get()
        text = convert(image_path)
        # Notify the queue that the "work item" has been processed.
        filesys.add_page(text)
        queue.task_done()
        logging.info(f'{name}|{queue} has processed {image_path}')



async def async_images_processor(image_paths, fs):
    queue = asyncio.Queue()
    max_workers = 3
    for image_path in image_paths:
        await queue.put(image_path)

    tasks = []
    for i in range(max_workers):
        task = asyncio.create_task(image_worker(f'worker-{i}', queue, fs))
        tasks.append(task)

    # Wait until the queue is fully processed.
    started_at = time.monotonic()
    await queue.join()
    total_slept_for = time.monotonic() - started_at

    # Cancel our worker tasks.
    for task in tasks:
        task.cancel()
    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*tasks, return_exceptions=True)
    print('====')
    print(f'3 workers completed in parallel for {total_slept_for:.2f} seconds')


async def convert_async(image_path):
    return aiopytesseract.image_to_string(image_path)

async def pdf_task_async_2(images_from_path, data_path, temp_dir, data):
    myfiles = filesystem.MyFileSystem(data_path=data_path, base_dir=temp_dir, data=data)
    myfiles.start()
    await async_images_processor(images_from_path, myfiles)
    pass

def pdf_task_2(images_from_path, data_path, temp_dir, data):
    myfiles = filesystem.MyFileSystem(data_path=data_path, base_dir=temp_dir, data=data)
    myfiles.start()
    for image_path in images_from_path:
        logging.info(f"Processing {image_path}")
        text = convert(image_path)
        myfiles.add_page(text)
    return myfiles.upload_to_s3()

async def pdf_task_async(images_from_path, data_path, temp_dir, data):
    return await asyncio.to_thread(pdf_task_2, images_from_path, data_path, temp_dir, data)


import random

def get_some_items(it, start, stop):
    return itertools.islice(it, start, stop)

def get_n_items(it, start, n):
    return get_some_items(it, start, start+n)

def get_next_n_items(it, current_item, n):
    return get_n_items(it, current_item, n)



def yield_json_paths():
    root = "json_data"
    for p in os.listdir(root):
        yield os.path.join(root, p)

import sys

def process_items():
    for item in yield_json_paths():
        done = False
        with open(item) as f:
            try:
                target = json.load(f)
            except Exception as e:
                target = None
                done = True
                logging.error(e)
                content = f.read()
                if not content.isspace():
                    logging.error(f"Problem json parsing this data \n {content} \n")
        if target is not None:
            while not done:
                logging.info(f"Processing {target['title']}")
                # dump_json_aws(target, aws_data_path)
                try:
                    process_item(target, item)
                except KeyboardInterrupt:
                    logging.info("Exit called while processing {target['title']}...")
                    if input("Do you want to stop? [Y/N]").lower() == 'y':
                        sys.exit(0)
                except Exception as e:
                    logging.error(f"Error while processing {target['title']}")
                    logging.error(e)
                    done = True




def process_item(item, p):
    logging.info(f'started {item}')
    old_url = "http://english.grimoar.cz"
    cool_url = "https://egvn9ieweb.execute-api.us-east-2.amazonaws.com"
    dl_target = item['download_url']
    dl_target = dl_target.replace(old_url, cool_url)
    try:
        process_pdf(dl_target, clean_title(item['title']) + ".pdf", p, data=item,
                           alt_url=item['download_url'])
    except Exception as e:
        logging.error(e)
