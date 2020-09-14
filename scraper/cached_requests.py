import requests
import sqlalchemy as db
import os, gzip, random
import logging
from typing import List, Dict
import time, json
from datetime import datetime, timedelta
import s3fs

PROFILE = os.getenv('AWSACC')

logging.basicConfig()
log = logging.getLogger()

def default_engine(host):
    return db.create_engine(host, poolclass=db.pool.NullPool)

class CReq():
    def __init__(self, engine: db.engine=None, cache_loc: str="", proxies: List[str]=None):
        super().__init__()
        self.dbs = {'engine':engine}
        self.s3 = s3fs.S3FileSystem(profile=PROFILE)
        if engine is not None:
            self.dbs['metadata'] = db.MetaData()
            self.dbs['pages'] = db.Table('pages', self.dbs['metadata'], autoload=True, autoload_with=engine)
        self.proxies = proxies
        self.cache_loc = cache_loc
        self.requests_session = requests.Session()
    
    def get_proxy(self):
        proxies = self.proxies
        proxy_dict = None
        if proxies is not None:
            proxy_ip = random.choice(proxies)
            proxy_dict = {'http':proxy_ip, 'https':proxy_ip}
        else: proxy_dict = None
        return proxy_dict

    def get_www(self, url):
        num_tries = 0
        while True:
            try:
                pxy = self.get_proxy()
                res = requests.get(url, proxies=pxy)
                # only raise status if code is not 404
                if res.status_code != 404: res.raise_for_status()
                break
            except (requests.exceptions.ProxyError, requests.exceptions.HTTPError, 
                    requests.exceptions.SSLError, requests.exceptions.ChunkedEncodingError) as e:
                log.warning(f"Get {num_tries+1}/10 failed with proxy on {url} and proxy {pxy['http']}")
                num_tries += 1
                if num_tries == 9:
                    raise
                time.sleep(1*num_tries) # increasing sleep time for each fail
        return res

    def post_www(self, url, data, request_headers=None):
        with self.requests_session as s:
            s.headers = request_headers
            s.proxies = self.get_proxy()
            r = s.post(url, data = data, headers=request_headers)
        return r

    def get_db_id(self, url, post_msg=None):
        pages = self.dbs['pages']
        query = db.select([pages.columns.id, pages.columns.headers, pages.columns.date_created]).\
            where(db.and_(pages.columns.url==url, 
                          pages.columns.status==200, 
                          pages.columns.post_msg==(db.null() if post_msg is None else json.dumps(post_msg, sort_keys=True))
                          )).\
                order_by(db.desc(pages.columns.date_created))
        self.dbs['engine'].dispose()
        with self.dbs['engine'].connect() as connection: 
            qry_exec = connection.execute(query)
            res = qry_exec.fetchall()
        
        return res
    
    def read_file(self, id:int, content_ext:str="html"):
        if self.cache_loc == "": return None
        path = os.path.join(self.cache_loc, str(id))
        res = t = type('', (), {})()
        res.file_loc = path
        try:
            with self.s3.open(f'{path}.gz.{content_ext}', 'rb') as f:
                g = gzip.GzipFile(fileobj=f)
                file_content = g.read()
            res.content = file_content
            res.status_code = 200
        except FileNotFoundError as e:
            log.warning(f"ID {str(id)} not found in store. Will redownload")
            res.status_code = 404
        return res

    def get_db(self, url, post_msg=None):
        res_list = self.get_db_id(url, post_msg)
        # TO DO: return multiple objects if more than 1 match
        all_res = []
        for db_id, db_headers, db_date_created in res_list: 
            if db_headers is not None:
                db_headers = {x.lower().strip():v for x,v in db_headers.items()}
                if db_headers['content-type'].find('html') > 0:
                    content_ext = 'html'
                elif db_headers['content-type'].find('json') > 0:
                    content_ext = 'json'
                else:
                    log.warning(f"extension {db_headers['content-type']} not implemented, saving wihout extension")
                    content_ext = ""
            else:
                content_ext = "html"
            
            res = self.read_file(db_id, content_ext)
            if res.status_code == 404: continue
            res.url = url
            res.headers = db_headers
            res.date_created = db_date_created
            if content_ext == "json":
                res.json = json.loads(res.content)
            all_res.append(res)
            
        return all_res

    def save_db(self, res, post_msg=None):
        if self.cache_loc == "": return None
        pages = self.dbs['pages']
        self.dbs['engine'].dispose()
        url_save = res.post_url if hasattr(res,'post_url') else res.url
        with self.dbs['engine'].connect() as connection: 
            query = db.insert(pages).values(url=url_save, status=res.status_code, 
            headers=dict(res.headers), 
            post_msg=db.null() if post_msg is None else json.dumps(post_msg, sort_keys=True)).returning(pages.columns.id)
            trans = connection.begin()
            try:
                dres = connection.execute(query)
                id_save = dres.fetchone()[0]
                path = os.path.join(self.cache_loc, str(id_save))
                with self.s3.open(f'{path}.gz.html', 'wb') as f:
                    res_gz = gzip.compress(res.content)
                    f.write(res_gz)
                trans.commit()
            except:
                log.warning(f"ID {str(id_save)} could not save, will not commit to db.")
                trans.rollback()
                raise
        return None
    
    def get(self, url, max_age_days=365) -> List[object]:
        """
        Gets a list of request objects from cache. 
        If max_age_days is 0, then also fetches the latest from www.
        The order is based on date fetched, so most recent is always first
        """
        # if no engine specified, act like normal requests
        if self.dbs['engine'] is None: return self.get_www(url)

        db_res = []
        www_res = []
        force_new = (max_age_days<1)

        db_res = self.get_db(url)
        if len(db_res):
            # check date created of most recent `get` to see if want to get a newer one
            if db_res[0].status_code == 404 or\
                db_res[0].date_created < (datetime.today() - timedelta(days=max_age_days)): 
                force_new = True
                
        if force_new or not len(db_res):
            log.info("Getting from www")
            www_res = self.get_www(url)
            www_res.headers['final_url'] = www_res.url
            www_res.url = url
            self.save_db(www_res)
            www_res = [www_res]

        return www_res + db_res
    
    def post(self, url, data:dict, request_headers=None, force_new=False):

        # if no engine specified, act like normal requests
        if self.dbs['engine'] is None: return self.post_www(url, data=data, request_headers=request_headers)

        res = None
        if not force_new: 
            res = self.get_db(url, post_msg=data)
            if res is not None and res.status_code == 404: force_new = True
        if res is None or force_new:
            log.info("Not found in database, getting from web")
            res = self.post_www(url, data=data, request_headers=request_headers)
            res.post_url = url
            self.save_db(res, post_msg=data)
        return res
