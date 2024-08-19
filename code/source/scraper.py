from bs4 import BeautifulSoup
import requests
import regex as re
from multiprocessing.pool import ThreadPool
from datetime import datetime, timedelta
import pandas as pd
import random
import time
from stqdm import stqdm
import json


def fillTo10D(cell):
    while len(cell) != 10:
        cell = '0' + cell
    return cell


def generate_CIK_TICKER(filename = 'ticker-SEC.csv'):
    jsonSEClist = APIconnector('https://www.sec.gov/files/company_tickers.json').get_request()
    recentFilings = pd.DataFrame.from_dict(jsonSEClist.json()).T
    recentFilings['cik10D'] = recentFilings['cik_str'].astype(str).apply(lambda x: fillTo10D(x))
    #recentFilings.to_csv(filename, index=False)
    
    return recentFilings
    
class APIconnector:
    def __init__(self, URL : str):
        self.URL = URL
        self.headers_list = [
                        "Java-http-client/ Mozilla/5.0 (X11; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0",
                        'Java-http-client/ Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36']
        self.headers = {'User-Agent' : random.choice(self.headers_list),
                        'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                        'Sec-Fetch-Dest' : 'iframe'}
        self.response = None
        
    def make_soup(self):
        if not self.response:
            self.response = self.get_request()
        soup = BeautifulSoup(self.response.content, "html.parser")
        return soup
    
    def get_request(self):
        if not self.response:
            self.response = requests.get(self.URL, headers = self.headers, timeout=1000)
        return self.response

def rename_cols_in_df(df):
    renameDic = {}
    for column in df.columns:
        newName = column.strip().replace(' ', '_').replace('-', '_')
        renameDic[column] = newName
        
    df.rename(columns=renameDic, inplace=True)
    return df



def monthWindow(df):    
    daysDiff = df['diffDate']
    months = 0
    while daysDiff >= 30:
        daysDiff -=30
        months +=1
        
    return months


def unpactUnitsJson(index,jsonDataframe):
    key = list(jsonDataframe['units'].iloc[index].keys())[0]
    valuesInTable = pd.DataFrame(jsonDataframe['units'].iloc[index][key])
        
    if 'start' in valuesInTable.columns:
        valuesInTable['startFormat'] = pd.to_datetime(valuesInTable['start'])
    
    if 'end' in valuesInTable.columns:
        valuesInTable['endFormat'] = pd.to_datetime(valuesInTable['end'])
        
    if 'end' in valuesInTable.columns and 'start' in valuesInTable.columns:
        valuesInTable['diffDate'] =  (valuesInTable['endFormat'] - valuesInTable['startFormat']).dt.days
        valuesInTable['monthWindow'] = valuesInTable.apply(monthWindow, axis=1)
        
    #valuesInTable['time'] = valuesInTable['endFormat'].dt.year.astype(str).str[2:] + valuesInTable['endFormat'].dt.month.map("{:02}".format)

    
    return valuesInTable


def unpackSECjson(cik):
    scr = APIconnector(f'https://data.sec.gov/api/xbrl/companyfacts/{cik}.json')
    
    jsonRequest = scr.get_request().json()
    r = jsonRequest['facts']['us-gaap']
    r = json.dumps(r)
    jsonDataframe = pd.read_json(r).T
    jsonDataframe.reset_index(inplace=True, names='finType')

    valuesDF = pd.DataFrame()
    for index, row in jsonDataframe.iterrows():
        unit = unpactUnitsJson(index, jsonDataframe)
        unit['finType'] = jsonDataframe.iloc[index]['finType']
        valuesDF = pd.concat([unit,valuesDF])
    
    mergedDF = jsonDataframe.merge(valuesDF, on='finType')
    mergedDF = mergedDF[['finType', 'val', 'accn', 'fy', #'label', 'description'
       'fp', 'form', 'filed', 'frame', 'endFormat', 'time', 'startFormat', 'monthWindow']]
    
    return mergedDF


def get_spy500_formWiki():
    table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
    table['CIK'] = table['CIK'].astype(str).apply(lambda x: fillTo10D(x))
    table = rename_cols_in_df(table)
    return table


def get_rangeOfDates(yearOffset):
    now = datetime.now()
    dateto = int(now.timestamp())
    dt = timedelta(days=366*yearOffset)
    datefrom = int((now - dt).timestamp())
    
    return dateto, datefrom

def get_StockPrices(ticker, interval = '1d', startSelect = None):
    if startSelect:
        end, start = get_rangeOfDates(24)
        start = int(datetime.strptime(startSelect,"%Y-%m-%d").timestamp())
    else:
        end, start = get_rangeOfDates(24)
    
    stockPrice = APIconnector(f'https://query2.finance.yahoo.com/v8/finance/chart/{ticker}?period1={start}&period2={end}&interval={interval}')
    
    adjClose = stockPrice.get_request().json()['chart']['result'][0]['indicators']['adjclose'][0]['adjclose']
    timestap = stockPrice.get_request().json()['chart']['result'][0]['timestamp']
    priceDF = pd.DataFrame({'adjClose':adjClose, 'time':timestap})
    priceDF['date'] =  pd.to_datetime(priceDF['time'].apply(lambda x: datetime.fromtimestamp(x))).dt.strftime('%Y-%m-%d')
    priceDF['ticker'] = ticker
    priceDF.drop('time', inplace=True, axis=1)
    
    priceDF["Quarter"] = "Q" + pd.to_datetime(priceDF['date']).dt.quarter.astype(str) + "-" \
                        + pd.to_datetime(priceDF['date']).dt.year.astype(str)
    groupByQ = priceDF[["Quarter", "adjClose"]].groupby("Quarter").mean().reset_index().rename(columns={'adjClose': 'meanADJclose'})
    mergeMean = pd.merge(priceDF, groupByQ, on = 'Quarter')

    return mergeMean

def cleaned_companyfacts(jsonDataframe):
    valuesDF = pd.DataFrame()
    for index, row in jsonDataframe.iterrows():
        unit = unpactUnitsJson(index, jsonDataframe)
        unit['finType'] = jsonDataframe.iloc[index]['finType']
        valuesDF = pd.concat([unit,valuesDF])
        
        
    mergedDF = jsonDataframe.merge(valuesDF, on='finType')
    mergedDF.drop(['units'], axis = 1, inplace=True)
    #mergedDF.fillna("null", inplace = True)
    
    return mergedDF

def get_companyfacts(cik):
    if "CIK" not in cik:
        cik = "CIK" + cik
    
    baseUrl = f'https://data.sec.gov/api/xbrl/companyfacts/{cik}.json'
    scr = APIconnector(baseUrl)
    jsonRequest = scr.get_request().json()
    r = jsonRequest['facts']['us-gaap']
    r = json.dumps(r)
    jsonDataframe = pd.read_json(r).T

    jsonDataframe.reset_index(inplace=True, names='finType')
    
    mergedDF = cleaned_companyfacts(jsonDataframe)
    
    mergedDF = mergedDF.apply(lambda x: addDateKey(x, 'end', 'start'), axis=1)

    
    return mergedDF

def get_CIK_by_Ticker(ticker, filename = 'ticker-SEC.csv', fill0 = True):
    
    recentFilings = pd.read_csv(filename)
    selectedTicker = recentFilings[recentFilings['ticker'] == ticker]

    if not fill0:
        return str(selectedTicker.cik_str.values[0])
    
    return fillTo10D(str(selectedTicker.cik_str.values[0]))


def addDateKey(row, col1, col2):

    try:
        row['yearMonthDay'] = pd.to_datetime(row[col1]).strftime("%Y%m%d")
    except ValueError:
        row['yearMonthDay'] = pd.to_datetime(row[col2]).strftime("%Y%m%d")
    except:
        row['yearMonthDay'] = ''
    
    return row


def get_SEC_filings(cik, ticker):
    if "CIK" not in cik:
        cik = "CIK" + cik

    clearCik = cik.replace('CIK', '')
    reqURL = F'https://data.sec.gov/submissions/CIK{clearCik}.json'
    scr = APIconnector(reqURL)

    scr.URL = reqURL
    res = scr.get_request()
    JSONresponse = res.json()

    #https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json
    filings = pd.DataFrame.from_dict(JSONresponse['filings']['recent'])

    filings['accessionNumberCLEAN'] = filings['accessionNumber'].apply(lambda x: x.replace('-', ''))
    filings['fileURL'] = 'https://www.sec.gov/Archives/edgar/data/' + clearCik + "/" + \
        filings['accessionNumberCLEAN'] + "/"+ filings['primaryDocument']
    
    filings.drop('accessionNumberCLEAN', inplace = True, axis = 1)
    filings = filings.apply(lambda x: addDateKey(x, 'reportDate', 'filingDate'), axis=1)
    #filings.fillna('null', inplace=True)
    #filings.replace('', 'null', inplace=True)
    filings['ticker'] = ticker
    
    return filings

if __name__=="main":
    pass