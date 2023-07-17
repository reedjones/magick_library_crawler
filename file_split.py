__author__ = "reed@reedjones.me"

import logging

logging.basicConfig(filename='splitter.log', encoding='utf-8', level=logging.DEBUG)
from scraper import load, flatten, append_to_aws, url_to_text, mark_ons3, check_on_s3
import pandas as pd
import json
import os


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


def process_items():
    for item in yield_json_paths():
        with open(item) as f:
            target = json.load(f)
            print(f"Processing {target['title']}")
            logging.debug(f"Processing {target['title']}")
            if not check_on_s3(target):
                result = process_item(target)
                target['document_text'] = result
                df = pd.DataFrame([target])
                append_to_aws(df)
                mark_ons3(target)


def process_item(item):
    target = item['download_url']
    return url_to_text(target)


if __name__ == '__main__':
    process_items()
