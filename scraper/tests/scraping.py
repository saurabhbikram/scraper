import sys
sys.path.append("..")
from cached_requests import *
from crawler import *
import shutil

S3PATH = 's3://sbcrawl-test/'
PROXIES = ['63.141.241.98:16001', '163.172.36.211:16001', '69.30.240.226:15001', '195.154.255.118:15001']

logging.basicConfig()

engine = db.create_engine('postgresql://prop@localhost/crawler_test')
crawler = CReq(engine, cache_loc=S3PATH, proxies=None)
crawler_proxy = CReq(engine, cache_loc=S3PATH, proxies = PROXIES)

def func(x):
    return crawler_proxy.get(x, max_age_days=0)

def test_multi_proc():
    list_of_urls = ["https://www.bbc.co.uk/sport",
    "http://www.bbc.co.uk/weather", "https://uk.yahoo.com"]

    crawl_urls(func, list_of_urls, threads=2)

def test_proxy():
    res = crawler.get("http://httpbin.org/ip", max_age_days=0)[0]
    res_proxy = crawler_proxy.get("http://httpbin.org/ip", max_age_days=0)[0]
    assert res.json()['origin'] != res_proxy.json()['origin']

def test_requests():
    """
    To read from www
    """
    res = crawler.get("https://stackoverflow.com/questions/19476816/creating-an-empty-object-in-python", max_age_days=0)[0]
    assert res.status_code == 200
    assert ~hasattr(res,"date_created")
    res = crawler.get("https://stackoverflow.com/questions/19476816/creating-an-empty-object-in-python", max_age_days=1)[0]
    assert hasattr(res,"date_created")   

def test_requests_NEW_age_day1():
    """
    To read from www an old page while asking to retreive from database if possible. then retreive the page again
     and make sure it comes from db
    """
    engine.dispose()
    with engine.connect() as con:
        con.execute("delete from pages where url = 'https://docs.python.org/3/tutorial/errors.html'")
    res = crawler.get("https://docs.python.org/3/tutorial/errors.html", max_age_days=1)[0]
    assert ~hasattr(res,"date_created")
    res = crawler.get("https://docs.python.org/3/tutorial/errors.html", max_age_days=1)[0]
    assert hasattr(res,"date_created")

def test_db_read_nofile():
    """
    Check that if file not found in cache, download it to cache.
    """
    res = crawler.get("https://stackoverflow.com/questions/19476816/creating-an-empty-object-in-python")[0]
    assert res.status_code == 200



