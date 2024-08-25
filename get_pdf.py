__author__ = "reed@reedjones.me"
from proxy_requests import ProxyRequests
import random

def random_url():
    i = random.choice(range(1, 2051))
    return f"http://english.grimoar.cz/?Loc=dl&Lng=2&Lng=2&Back=key&UID={i}"


test_url = "http://english.grimoar.cz/?Loc=dl&Lng=2&Lng=2&Back=key&UID=865"


def get_pdf(url):
    r = ProxyRequests(url)
    resp = r.get()

