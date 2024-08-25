__author__ = "reed@reedjones.me"

import logging

from datastore import dump_json_aws
from ocr import get_title_dir_path, get_title_data_path
import asyncio
logging.basicConfig(filename='splitter.log', encoding='utf-8', level=logging.DEBUG)
from scraper import load, flatten, url_to_text, mark_ons3, check_on_s3
import json
import os

old_url = "http://english.grimoar.cz"
cool_url = "https://egvn9ieweb.execute-api.us-east-2.amazonaws.com"


def get_the_data(result_file):
    data = load(result_file)
    for item in flatten(data):
        yield item


def dump_json(data, fname):
    fpath = os.path.join("json_data", fname)
    with open(fpath, "w+") as outf:
        json.dump(data, outf, default=lambda x: str(x))


def split_document_texts(result_file):
    current_num = 1
    for item in get_the_data(result_file):
        fname = f"item_{current_num}.json"
        dump_json(item, fname)
        current_num += 1


def yield_json_paths():
    root = "json_data"
    names = os.listdir(root)
    for name in names:
        yield os.path.join(root, name)


async def process_items():
    for item in yield_json_paths():
        with open(item) as f:
            target = json.load(f)
            print(f"Processing {target['title']}")
            logging.debug(f"Processing {target['title']}")
            # dump_json_aws(target, aws_data_path)
            if not check_on_s3(target) and target['lang'] in ['en', '', '?', ' '] and 'Feminist Gospel' not in target['title']:
                await process_item(target)


async def process_item(item):
    dl_target = item['download_url']
    dl_target = dl_target.replace(old_url, cool_url)
    aws_data_path = item['data_path']
    target_doc_path = item['data_documents_path']
    print(f"will request {dl_target}")
    try:
        result = url_to_text(dl_target, item['title'])
        item['text_documents'] = result['documents']
        mark_ons3(item)
        await dump_json_aws(item, aws_data_path)

    except Exception as e:
        print(e)
        logging.error(e)



if __name__ == '__main__':
    process_items()
