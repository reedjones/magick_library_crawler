__author__ = "reed@reedjones.me"

import json
import os
from typing import Union

import requests
from ballyregan import ProxyFetcher, Proxy
from bson import json_util

PROXY_FILE = "proxies.txt"
PROXY_INFO_PATH = "proxies"
PROXY_INFO_FILE = "proxies.json"
INFO_FILE = os.path.join(PROXY_INFO_PATH, PROXY_INFO_FILE)
from datetime import datetime


def setup_dirs():
    if not os.path.isdir(PROXY_INFO_PATH):
        os.mkdir(PROXY_INFO_PATH)
    if not os.path.isfile(INFO_FILE):
        _data = {}
        with open(INFO_FILE, 'w+') as f:
            json.dump(_data, f)


def get_proxy_info(proxy=None):
    with open(INFO_FILE) as f:
        data = json.load(f, object_hook=json_util.object_hook)
    if proxy:
        return data.get(proxy, None)
    return data


def save_proxy_info(proxy, info):
    data = get_proxy_info()
    data[proxy] = info
    with open(INFO_FILE) as f:
        json.dump(data, f, default=json_util.default)


def load_proxy_text():
    with open(PROXY_FILE, 'r') as f:
        data = f.readlines()
    return [d for d in data if d and not d.isspace()]


def get_proxies(n=None, debug=False):
    fetcher = ProxyFetcher(debug=debug)
    if n:
        proxies = fetcher.get(limit=n)
    else:
        proxies = [fetcher.get_one()]
    return proxies


class Fetcher(object):
    SECONDS_WAIT = 60 * 3

    def __str__(self):
        return str(self.proxy)

    def __init__(self,
                 proxy: Union[Proxy, tuple, list, str],
                 wait_time=SECONDS_WAIT):
        self.proxy = None
        self.wait_time = wait_time
        self.last_request_time = datetime.now()
        self.request_count = 0
        self.setup(proxy)
        self.save()

    @property
    def granted(self):
        return self._is_granted()

    def _is_granted(self):
        if self.request_count == 0:
            return True
        return self.elapsed_time() > self.wait_time

    def elapsed_time(self):
        current_time = datetime.now()
        elapsed = current_time - self.last_request_time
        return elapsed.total_seconds()

    def load(self):
        return get_proxy_info(str(self))

    def dump(self):
        save_proxy_info(str(self), self.to_dict())

    def to_dict(self):
        d = {
            'granted': self._is_granted(),
            'last_request_time': self.last_request_time,
            'request_count': self.request_count,
            'proxy': str(self)
        }
        return d

    @classmethod
    def from_dict(cls, d):
        if 'proxy' not in d:
            raise KeyError("proxy must be in dictionary")
        p = d.pop('proxy')
        fetcher = cls(p)
        fetcher.fdict(d)
        return fetcher

    def fdict(self, d):
        for k, v in d.items():
            setattr(self, k, v)

    def setup(self, proxy):
        if isinstance(proxy, str):
            self.proxy = proxy
        elif isinstance(proxy, tuple) or isinstance(proxy, list):
            self.proxy = f"{proxy[0]}:{proxy[1]}"
        else:
            self.proxy = proxy

        if d := self.load():
            self.fdict(d)

    def save(self):
        save_proxy_info(str(self), self.to_dict())


    def get(self, url):
        p = {'https': str(self)}
        return requests.get(url, proxies=p, allow_redirects=True)


def main():
    pass


if __name__ == '__main__':
    main()
