__author__ = "reed@reedjones.me"

import json
import logging
import os
import shutil
import zipfile
from logging.handlers import RotatingFileHandler

BUCKET_NAME = "magicdocuments"
import boto3
from botocore.exceptions import ClientError
import asyncio

logging.basicConfig(
    level=logging.INFO,
    format="[FILESYSTEM] %(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        RotatingFileHandler("filesystem.log", maxBytes=1000000),
        logging.StreamHandler()
    ]
)

class NotDone(Exception):
    pass

def copy_to_dir(what, where):

    shutil.copyfile(what, where)


def clean_title(title: str):
    title = "".join([c for c in title if c.isalpha() or c.isdigit() or c == ' '])
    return title.replace(" ", "_").strip()





def upload_file(file_name, bucket=BUCKET_NAME, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def add_to_not_done(fpath):
    logging.warning(f"{fpath} not done, stashing it for later...")
    if not os.path.isdir("not_done"):
        os.mkdir("not_done")
    copy_to_dir(fpath, "not_done")


async def upload_file_async(file_name, bucket=BUCKET_NAME, object_name=None):
    return asyncio.to_thread(upload_file, file_name, bucket, object_name)


class MyFileSystem(object):
    """
    This class creates a temporary directory for a book title
    it takes in page contents one by one and creates numbered files in that directory based on order received
    eg:
    - {BookTitle}Directory
        -- page_0001.txt
        -- page_0002.txt
        -- page_0003.txt

    methods:
        - _zip_pages: this method takes all the page_nnnn.txt files and zips them into an archive called 'pages'
        - _create_temp_dir: this method creates the temporary directory that will be used
        - upload_to_s3: this method compresses the entire book directory and uploads it to s3

    """

    def __init__(self, data_path=None, bucket_name=BUCKET_NAME, base_dir="books", data=None):
        self.base_dir = base_dir
        self.data = data
        self.book = data
        self.book_title = clean_title(self.book['title'])
        self.temp_dir = None
        self.pages = []
        self.temp_dir_name = os.path.join(self.base_dir, f"{self.book_title}Directory")
        self.zip_output_name = os.path.join(self.base_dir, f"{self.book_title}Directory.zip")
        self.pages_zip_name = os.path.join(self.base_dir, self.temp_dir_name, 'pages.zip')
        self.page_count = 1
        self.bucket = bucket_name
        self.data_path = data_path

    def _create_temp_dir(self):
        # Create a temporary directory for the book
        self.temp_dir = self.temp_dir_name
        os.makedirs(self.temp_dir, exist_ok=True)
        logging.info("created dir")

    def _get_page_name(self):
        return f"page_{self.page_count:04}.txt"

    def add_page(self, content):
        if not isinstance(content, str):
            try:
                content = str(content)
            except Exception as e:
                logging.error(e)
                return
        if not content or content.isspace():
            return
        pagename = self._get_page_name()
        fname = os.path.join(self.temp_dir, pagename)
        with open(fname, "w+") as out:
            out.write(content)
        self.page_count += 1
        self.pages.append(pagename)

    def _zip_self(self):
        return shutil.make_archive(self.zip_output_name, 'zip', self.temp_dir_name)

    def finish(self, result, output):
        # os.remove(self.zip_output_name)
        # shutil.rmtree(self.temp_dir_name)
        # self.temp_dir = None
        success = True
        if not result or result is None:
            success = False
            add_to_not_done(self.data_path)
            add_to_not_done(output)
        os.remove(self.data_path)
        return success

    def start(self):
        self._create_temp_dir()
        # await copy_to_dir(self.data_path, self.temp_dir_name)

    def upload_to_s3(self):
        # Zip the book directory
        # self._zip_pages()
        output = self._zip_self()
        # Upload the book archive to S3
        result = upload_file(output, self.bucket)
        return self.finish(result, output)

    async def upload_to_s3_async(self):
        output = self._zip_self()
        result = await upload_file_async(output, self.bucket)
        return self.finish(result, output)


class PDFJob(object):
    def __init__(self, fs, pdf_path):
        self.fs = fs



class AsyncFileManager:
    def __init__(self, data_path=None, bucket_name=BUCKET_NAME, base_dir=None, data=None):
        self.files = MyFileSystem(data_path=data_path, bucket_name=bucket_name, base_dir=base_dir, data=data)

    async def __aenter__(self):
        print('enter method called')
        self.files.start()
        logging.info("started")
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        print('exit method called')
        r = await self.files.upload_to_s3_async()
        return r

    def __enter__(self):
        print('enter method called')
        self.files.start()
        logging.info("started")
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        print('exit method called')
        return self.files.upload_to_s3()



"""
# define a simple coroutine
async def custom_coroutine():
    # create and use the asynchronous context manager
    async with AsyncContextManager() as manager:
        # report the result
        print(f'within the manager')
 
# start the asyncio program
asyncio.run(custom_coroutine())
"""
