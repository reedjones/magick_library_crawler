__author__ = "reed@reedjones.me"

import os.path
import re
import time
from pathlib import Path
from tempfile import TemporaryDirectory
import asyncio
import aiohttp as aiohttp
import pytesseract
import requests
import s3fs
from PIL import Image
from pdf2image import pdfinfo_from_path, convert_from_path

import uuid
from ratelimit import limits, sleep_and_retry, RateLimitException
import logging

logging.basicConfig(filename='ocr.log', encoding='utf-8', level=logging.DEBUG)
AWS_FS = s3fs.S3FileSystem()
from backoff import on_exception, expo

ERROR_DOC = "Error Fetching Document"


class APIFailed(Exception):
    pass


EIGHT_MINUTES = 60 * 8

import boto3
import uuid
s3_client = boto3.client('s3')

def fetch_pdf(url):
    r = requests.get(url, allow_redirects=True)
    if r.status_code != 200:
        raise APIFailed('API response: {}'.format(r.status_code))
    return r


def try_to_fetch(url, tries=1):
    try:
        r = fetch_pdf(url)
        return r
    except Exception as e:
        logging.debug(e)
        print(e)
        time.sleep(9*tries)
        if tries >= 3:
            return None
        try_to_fetch(url, tries+1)


def is_downloadable(url):
    """
    Does the url contain a downloadable resource
    """
    h = requests.head(url, allow_redirects=True)
    header = h.headers
    content_type = header.get('content-type')
    if 'text' in content_type.lower():
        return False
    if 'html' in content_type.lower():
        return False
    return True


def get_filename_from_cd(cd):
    """
    Get filename from content-disposition
    """
    if not cd:
        return None
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return None
    return fname[0]


def process_image(image_file_path, pos):
    logging.debug('Processing Image...')

    text = str((pytesseract.image_to_string(Image.open(image_file_path))))
    return {
        'text': text,
        'pos': pos
    }


def clean_title(title: str):
    title = "".join([c for c in title if c.isalpha() or c.isdigit() or c == ' '])
    return title.replace(" ", "_").strip()


def get_title_dir_path(title):
    title = clean_title(title)
    return f"magicdocuments/documents/{title}"


def get_title_data_path(title):
    title = clean_title(title)
    return f"magicdocuments/documents/{title}.json"


def make_title_dir(title):
    dir_path = get_title_dir_path(title)
    AWS_FS.mkdir(dir_path)



def write_title_page(title, page, num=1, num2=1):
    dir_path = get_title_dir_path(title)
    if not AWS_FS.exists(dir_path):
        AWS_FS.mkdir(dir_path)
    filename = f"{title}_page_{num:03}_part_{num2}.txt"
    full_file_path = f"{dir_path}/{filename}"
    if not AWS_FS.exists(full_file_path):
        s3_client.upload_file()

        with AWS_FS.open(full_file_path, "w") as outfile:
            outfile.write(page)
    else:
        print(f"Skipping {filename} already there")
    return full_file_path


def url_to_text(url, book_title):
    print(f"Started {book_title}")
    result = {
        'documents': [],
        'data_path': get_title_dir_path(book_title)

    }
    with TemporaryDirectory() as tempdir:
        print(f"Created temp dir {tempdir} \n trying to fetch")
        r = try_to_fetch(url)
        if not r:
            return result
        print("fetched success")
        target_filename = get_filename_from_cd(r.headers.get('content-disposition'))
        if not target_filename:
            target_filename = str(uuid.uuid4()) + ".pdf"
        target_filename = "".join([c for c in target_filename if c.isalpha() or c.isdigit() or c == ' ']).rstrip()

        output_path = Path(tempdir) / target_filename
        ct = r.headers.get('content-type', None)
        if ct:
            if 'html' in ct.lower() or 'text' in ct.lower():
                return result

        with open(output_path, 'wb') as outfile:
            outfile.write(r.content)

        logging.debug(f"got file {target_filename} and wrote to {output_path} now converting")
        print(f"got file {target_filename} and wrote to {output_path} now converting")
        pdf_info = pdfinfo_from_path(str(output_path))
        max_pages = pdf_info["Pages"]
        for page_numb in range(1, max_pages + 1, 10):
            pdf_pages = convert_from_path(output_path, dpi=200, first_page=page_numb, last_page=min(page_numb + 10 - 1, max_pages))

            print(f"converted chunk {page_numb} now enumerating")
            image_file_list = []
            for page_enumeration, page in enumerate(pdf_pages, start=1):
                # enumerate() "counts" the pages for us.
                # Create a file name to store the image
                filename = f"section_{page_numb}_page_{page_enumeration:03}.jpg"
                print(f"got page {filename}")
                image_file_path = Path(tempdir) / filename
                logging.debug(f"writing {image_file_path}")
                page.save(image_file_path, "JPEG")
                text = str((pytesseract.image_to_string(Image.open(image_file_path))))
                aws_filename = write_title_page(book_title, text, page_enumeration, page_numb)
                result['documents'].append(aws_filename)
    return result


if __name__ == '__main__':
    u = "http://english.grimoar.cz/?Loc=dl&Lng=2&Lng=2&Back=key&UID=865"
    t = url_to_text(u)
    logging.debug(t)
