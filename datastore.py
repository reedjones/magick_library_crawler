__author__ = "reed@reedjones.me"

import logging
import os
import pickle

import boto3
import pandas as pd
import pyarrow as pa

logging.basicConfig(filename='datastore.log', encoding='utf-8', level=logging.DEBUG)

BUCKET_NAME = "magicdocuments"
client = boto3.client('s3')
already_on_s3 = "processed_items_new_blue.pickle"
s3_data_key = "MagicDocuments2.parquet"
s3_data_filepath = "magicdocuments/MagicDocuments2.parquet"

import s3fs

AWS_FS = s3fs.S3FileSystem()

import json

HAVE_WROTE = False


async def dump_json_aws(data, path):
    json.dump(data, AWS_FS.open(path, 'w'), ensure_ascii=False )


def append_to_aws(df):
    if not AWS_FS.isfile(s3_data_filepath):
        logging.debug("no file there")
    else:
        current_data = load_from_aws()
        logging.debug(f"Loaded {len(current_data.index)} Items from AWS")
        df = pd.concat([current_data, df])
    with AWS_FS.open(s3_data_filepath, 'wb') as f:
        df.to_parquet(f)


def write_to_aws(df):
    with AWS_FS.open(s3_data_filepath, 'wb') as f:
        df.to_parquet(f)


def load_from_aws():
    with AWS_FS.open(s3_data_filepath, 'rb') as f:
        data = pd.read_parquet(f)
    return data


def append_to_aws2(items):
    data = load_from_aws()
    new_df = pd.DataFrame(items)
    combined = pd.concat([data, new_df])
    write_to_aws(combined)


def load(w):
    if not os.path.isfile(w):
        data = []
    else:
        with open(w, 'rb') as f:
            data = pickle.load(f)
    logging.debug(f"{w} has {len(data)} items...")
    return data


def dump(w, data, unique=False):
    if not data:
        data = []
    if unique:
        data = list(set(data))
    with open(w, 'wb') as f:
        pickle.dump(data, f)


def finished_url(u, finished="finished.pickle"):
    p = load(finished)
    p.append(u)
    dump(finished, p)


def problem_url(u, problem='problem.pickle'):
    p = load(problem)
    p.append(u)
    dump(problem, p)


def store_data(data, replace=False, results='results.pickle', unique=False):
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


def check_unique(item):
    d = load(already_on_s3)
    return item['title'] in d


def final_finish(item):
    d = load(already_on_s3)
    d.append(item['title'])
    dump(d, already_on_s3)


def store_s3(data, key):
    result = client.put_object(
        Body=str(json.dumps(data)),
        Bucket=BUCKET_NAME,
        Key=key,
    )


def load_s3(key):
    s3 = boto3.resource('s3')
    content_object = s3.Object(BUCKET_NAME, key)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content


def to_df(data):
    return pd.DataFrame(data)


def to_table(df):
    return pa.Table.from_pandas(df)


def add_items_to_table(item_list):
    new_data_frame = pd.DataFrame(item_list)
