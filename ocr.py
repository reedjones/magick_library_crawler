__author__ = "reed@reedjones.me"

import os.path
import re
from pathlib import Path
from tempfile import TemporaryDirectory

import pytesseract
import requests
from PIL import Image
from pdf2image import convert_from_path
import uuid
from ratelimit import limits, sleep_and_retry
import logging
logging.basicConfig(filename='ocr.log', encoding='utf-8', level=logging.DEBUG)


ERROR_DOC = "Error Fetching Document"

class APIFailed(Exception):
    pass

EIGHT_MINUTES = 60 * 8
@sleep_and_retry
@limits(calls=1, period=EIGHT_MINUTES)
def fetch_pdf(url):
    r = requests.get(url, allow_redirects=True)
    if r.status_code != 200:
        raise APIFailed('API response: {}'.format(r.status_code))
    return r


def try_to_fetch(url):
    try:
        r = fetch_pdf(url)
        return r
    except APIFailed as e:
        logging.debug(e)




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
        'text':text,
        'pos':pos
    }



def url_to_text(url):
    result = ""
    with TemporaryDirectory() as tempdir:
        try:
            r = fetch_pdf(url)
        except APIFailed as e:
            logging.debug(e)
            return ERROR_DOC
        target_filename = get_filename_from_cd(r.headers.get('content-disposition'))
        if not target_filename:
            target_filename = str(uuid.uuid4()) + ".pdf"
        target_filename = "".join([c for c in target_filename if c.isalpha() or c.isdigit() or c == ' ']).rstrip()

        output_path = Path(tempdir) / target_filename

        with open(output_path, 'wb') as outfile:
            outfile.write(r.content)

        logging.debug(f"got file {target_filename} and wrote to {output_path} now converting")
        pdf_pages = convert_from_path(
            output_path
        )
        image_file_list = []
        for page_enumeration, page in enumerate(pdf_pages, start=1):
            # enumerate() "counts" the pages for us.

            # Create a file name to store the image
            filename = f"page_{page_enumeration:03}.jpg"
            image_file_path = Path(tempdir) / filename
            logging.debug(f"writing {image_file_path}")
            page.save(image_file_path, "JPEG")
            image_file_list.append(image_file_path)

        for image_file in image_file_list:
            text = str((pytesseract.image_to_string(Image.open(image_file))))
            logging.debug(f"got text {text}")
            text = text.replace("-\n", "")
            # Finally, write the processed text to the file.
            result += text + "\n"
    return result


if __name__ == '__main__':
    u = "http://english.grimoar.cz/?Loc=dl&Lng=2&Lng=2&Back=key&UID=865"
    t = url_to_text(u)
    logging.debug(t)
