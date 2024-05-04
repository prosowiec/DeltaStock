from bs4 import BeautifulSoup
import requests
import regex as re
from multiprocessing.pool import ThreadPool
from datetime import datetime, timedelta
import pandas as pd
import random
import time
from stqdm import stqdm


def fillTo10D(cell):
    while len(cell) != 10:
        cell = '0' + cell
    return cell


def generate_CIK_TICKER(filename = 'ticker-SEC.csv'):
    jsonSEClist = APIconnector('https://www.sec.gov/files/company_tickers.json').get_request()
    recentFilings = pd.DataFrame.from_dict(jsonSEClist.json()).T
    recentFilings['cik10D'] = recentFilings['cik_str'].astype(str).apply(lambda x: fillTo10D(x))
    recentFilings.to_csv(filename, index=False)

class APIconnector:
    def __init__(self, URL : str):
        self.URL = URL
        self.headers_list = [
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
                        "Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0",
                        "Mozilla/5.0 (Windows NT 10.0; rv:78.0) Gecko/20100101 Firefox/78.0",
                        "Mozilla/5.0 (X11; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0",
                        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
                        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
                        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36']
        self.headers = {'User-Agent' : random.choice(self.headers_list) }
        
    def make_soup(self):
        r = self.get_request()
        soup = BeautifulSoup(r.content, "lxml")
        return soup
    
    def get_request(self):
        return requests.get(self.URL, headers = self.headers, timeout=1000)
    


if __name__=="main":
    pass