from multiprocessing.pool import Pool
import tqdm

def crawl_urls(func, urls, threads=2):
    if threads > 1:
        with Pool(processes=threads) as p:
            res = list(tqdm.tqdm(p.imap(func, urls), total=len(urls)))
    else:
        res = []
        for u in tqdm.tqdm(urls): res.append(func(u))
    return res
