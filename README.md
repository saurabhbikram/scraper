# Scraper

Easier scraping of web data.

## Motivation
A major pain point in doing data science on web data is the parsing of html into structured data. 

The parsing is a trial and error process, yet we do not want to grab the html from the web everytime the parser is changed. This library works on top of requests to cache *get* and *post* in a database/file system based on the url and post data. The user makes a request based on *url* as usual but the library intelligently decides if it wants to read from disk or web. This can be approx 10x quicker than reading from www all the time.

## How it works
Uses a SQLAlchemy backend datastore to cache requests metadata. 

The url, headers, status code of the endpoint and post data is stored in database against a primary key. The payload is stored on disk with the primary id as the filename. 

If a request is made and it matches the database and its within the max age specified then the payload is served from cache.

**Note**  All `post()` requests are made in a common session (unlike `requests`)

## Usage

Setup a sql database (see [database](scraper/db.sql)) and then

```
from cached_requests import CReq, default_engine

DATABASE =  'postgresql://user@localhost/db' # sqlalchemy connection string
STORE = "tmp" # file path to store payload
PROXIES = [] # list of proxies, chosen randomly at each request

requests = CReq(engine=default_engine(DATABASE), 
                        proxies=PROXIES, 
                        cache_loc=STORE)

requests.get("http://www.bbc.co.uk") # will grab from www
requests.get("http://www.bbc.co.uk") # will grab from disk

requests.get("http://www.bbc.co.uk", max_age_days=0) # force to get from www
```


### Multiprocessing 

You can use `Pool` to crawl multiple urls. See below

```
from crawler_config import requests # CReq object

from crawler import crawl_urls

def func(url):
    page = requests.get(url)
    # do something with the page, eg extraction etc
    res = page.xpath('//text())
    return res

crawl_urls(func, URLS_CRAWL, threads=2)
```