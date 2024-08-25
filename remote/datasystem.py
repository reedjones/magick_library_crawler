__author__ = "reed@reedjones.me"

import aiopytesseract
import json
import logging
import os
import pickle
import shutil
import tempfile
import time
import sys
import random
from logging.handlers import RotatingFileHandler
from pathlib import Path
import itertools
import filesystem
import requests
from PIL import Image
from pdf2image import pdfinfo_from_path, convert_from_path
from pytesseract import pytesseract


def load(w):
    if not os.path.isfile(w):
        data = []
    else:
        with open(w, 'rb') as f:
            data = pickle.load(f)
    logging.debug(f"{w} has {len(data)} items...")
    return data


def dump(w='dump.pickle', data=None, unique=False):
    if not data:
        return
    if not isinstance(data, list):
        data = [data]
    if unique:
        data = list(set(data))
    with open(w, 'wb') as f:
        pickle.dump(data, f)


def store(data, replace=False, results='store.pickle', unique=False):
    if replace:
        dump(results, data, unique=unique)
        logging.debug(f"Replaced {results}")
        return
    logging.debug(f"Writing to {results}")
    p = load(results)
    if isinstance(data, list):
        p += data
    else:
        p.append(data)
    dump(results, p, unique=unique)
    return


def resolve_url(u):
    old_url = "http://english.grimoar.cz"
    cool_url = "https://egvn9ieweb.execute-api.us-east-2.amazonaws.com"
    u2 = u.replace(old_url, cool_url)
    u3 = u.replace(cool_url, old_url)
    return [u, u2, u3]


def is_finished(item):
    title = item['title']
    dl = item['download_url']
    dldata = load('over2.pickle')
    data = load('over.pickle')
    data3 = load('skipped.pickle')
    data4 = load('skipped2.pickle')
    if title in data or dl in dldata or title in data3 or dl in data4:
        return True
    possible_urls = resolve_url(dl)
    for u in possible_urls:
        if u in dldata or u in data4:
            return True
    return False


HUMAN_DEL = True


def mark_finished(item):
    print(f"finishing {item}")
    title = item['title']
    dl = item['download_url']
    store(dl, results='over2.pickle', unique=True)
    store(title, results='over.pickle', unique=True)


def mark_skipped(item):
    print(f"Skipping {item}")
    title = item['title']
    dl = item['download_url']
    store(dl, results='skipped.pickle', unique=True)
    store(title, results='skipped2.pickle', unique=True)


def remove_the_item(data, path):
    print(f"removing {data}")
    shutil.rmtree(path)
    title = data['title']
    mark_finished(data)
    mark_skipped(data)
    print(f"removed {title}")


def skip_the_item(data, path):
    print(f"Skipping {data}")
    if not os.path.isdir('skipped'):
        os.mkdir('skipped')
    fdr = os.path.join('skipped', path)

    shutil.copyfile(path, fdr)
    os.remove(path)
    mark_skipped(data)


def deligate(target, item):
    data = target
    data_path = item
    dldata = load('over2.pickle')
    dldata2 = load('over.pickle')
    print(f"Processing {data} \n path {data_path} this is what i have:")

    for i in dldata:
        print(f"\t - {i}")
    for i2 in dldata2:
        print(f"\t - {i2}")

    response = input(f"Is this approp? y = yes, s = stop asking n = exit")
    if response in ['y', 'Y']:
        pass
    if response in ['n', 'N']:
        exit(1)
    if response in ['s', "S"]:
        HUMAN_DEL = False


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




def process_pdf(url, filename, data_path, data=None, alt_url=None):
    if data:
        if is_finished(data):
            remove_the_item(data, data_path)
            os.remove(data_path)
            return
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




def convert(image_path):
    return pytesseract.image_to_string(Image.open(image_path))


async def image_worker(name, queue, filesys):
    while True:
        # Get a "work item" out of the queue.
        image_path = await queue.get()
        text = await aiopytesseract.image_to_string(image_path)
        # Notify the queue that the "work item" has been processed.
        filesys.add_page(text)
        queue.task_done()
        logging.info(f'{name}|{queue} has processed {image_path}')


async def convert_async(image_path):
    return await aiopytesseract.image_to_string(image_path)


def pdf_task_2(images_from_path, data_path, temp_dir, data):
    myfiles = filesystem.MyFileSystem(data_path=data_path, base_dir=temp_dir, data=data)
    myfiles.start()
    for image_path in images_from_path:
        logging.info(f"Processing {image_path}")
        text = convert(image_path)
        myfiles.add_page(text)
    return myfiles.upload_to_s3()


def get_some_items(it, start, stop):
    return itertools.islice(it, start, stop)


def get_n_items(it, start, n):
    return get_some_items(it, start, start + n)


def get_next_n_items(it, current_item, n):
    return get_n_items(it, current_item, n)


def yield_json_paths():
    root = "json_data"
    d = os.listdir(root)
    random.shuffle(d)
    for p in d:
        yield os.path.join(root, p)




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
            if HUMAN_DEL:
                deligate(target, item)
            if is_finished(target):
                remove_the_item(target, item)
                continue
            else:
                if not target.get('lang', '?') in ['en', 'EN', ' ']:
                    skip_the_item(target, item)
                    continue
            while not done:
                logging.info(f"Processing {target['title']}")
                # dump_json_aws(target, aws_data_path)
                try:
                    process_item(target, item)
                    # process_item(target, item)
                    mark_finished(item)
                    mark_skipped(item)
                    remove_the_item(target, item)
                    done = True
                except KeyboardInterrupt:
                    logging.info("Exit called while processing {target['title']}...")
                    if input("Do you want to stop? [Y/N]").lower() == 'y':
                        sys.exit(0)
                except Exception as e:
                    logging.error(f"Error while processing {target['title']}")
                    logging.error(e)
                    skip_the_item(target, item)
                    mark_skipped(target)

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
        mark_finished(item)
        mark_skipped(item)
        remove_the_item(item, p)
    except Exception as e:
        mark_skipped(item)
        logging.error(e)
