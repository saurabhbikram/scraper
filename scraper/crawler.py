from multiprocessing.pool import ThreadPool
import tqdm

def crawl_urls(func, urls, threads=2, desc=""):
    if threads > 1:
        with ThreadPool(processes=threads) as p:
            res = list(tqdm.tqdm(p.imap(func, urls), total=len(urls), desc=desc))
    else:
        res = []
        for u in tqdm.tqdm(urls, desc=desc): res.append(func(u))
    return res
