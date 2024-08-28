from spark_deltalake.spark_operations import sparkDelta
from source.scraper import generate_CIK_TICKER, get_spy500_formWiki, \
    fillTo10D, get_SEC_filings, get_companyfacts, get_StockPrices
from tqdm import tqdm
import pandas as pd
from multiprocessing.pool import ThreadPool
import asyncio
import time
import multiprocessing
import numpy as np
from pyspark.sql.functions import col 
import traceback

def write_ticker_sec(sparkclass : sparkDelta, filename = 'ticker-sec'):
    sec_ticker = generate_CIK_TICKER()
    sparkclass.save_small_DeltaTable(sec_ticker, filename)
    print("Saved table {}".format(filename))
    
def write_spy500(sparkclass : sparkDelta, filename = 'spy500'):
    wikispy = get_spy500_formWiki()
    sparkclass.save_small_DeltaTable(wikispy, filename)
    print("Saved table {}".format(filename))
    
def get_cik_by_ticker(spark : sparkDelta, ticker):
    df = spark.get_spark_dataframe('ticker-sec', toDataframe=True)
    selectedTicker = df[df['ticker'] == ticker]

    return fillTo10D(str(selectedTicker.cik_str.values[0]))


def append_SEC_filings(sparkClass : sparkDelta, ticker, cik = None):
    if not cik:
        cik = get_cik_by_ticker(sparkClass, ticker)
    filings = get_SEC_filings(cik, ticker)
    sparkClass.save_partition_DeltaTable(filings, 'SEC_filings', 'ticker','append')
    

def append_company_facts(sparkClass : sparkDelta, ticker, cik = None):
    if not cik:
        cik = get_cik_by_ticker(sparkClass, ticker)
    companyfacts = get_companyfacts(cik)
    sparkClass.save_partition_DeltaTable(companyfacts, 'company_facts','accn', 'append')

def ingest_Facts_Fillings(sparkClass : sparkDelta, ticker, cik):
    append_company_facts(sparkClass, ticker, cik)
    append_SEC_filings(sparkClass, ticker, cik)
    append_StockPrices(sparkClass, ticker)
    
    
def get_ticker_bookmark_values(sparkClass : sparkDelta, ticker):
    df = sparkClass.get_min_max_fillings_date(ticker)
    resDic = {'max_time':df['max_time'].values[0], 'min_time': df['min_time'].values[0]}
    
    return resDic

def append_StockPrices(sparkClass : sparkDelta, ticker):
    bookmarDates = get_ticker_bookmark_values(sparkClass, ticker)
    prices = get_StockPrices(ticker, startSelect=bookmarDates['min_time'], endSelect=bookmarDates['max_time'])
    sparkClass.save_partition_DeltaTable(prices, 'stock_prices', 'ticker','append')


def load_spy500(sparkClass:sparkDelta, start_index = -1, end_index = 99999):
    ciks = sparkClass.get_spark_dataframe('spy500', toDataframe=True)
    for index, row in tqdm(ciks.iterrows()):
        if index >= start_index and index <= end_index:
            ingest_Facts_Fillings(sparkClass, row['Symbol'], row['CIK'])
        
    #load stock from ingested tickers    
    #ingestedStock = sparkClass.get_ingested_tickers()
    #for ticker in ingestedStock:
    #    append_StockPrices(sparkClass, ticker)


async def async_get_companyfacts(cik):
    return await asyncio.to_thread(get_companyfacts, cik)

async def async_get_SEC_filings(cik, ticker):
    return await asyncio.to_thread(get_SEC_filings, cik, ticker)

async def async_get_StockPrices(ticker, minPeriod, maxPeriod):
    return await asyncio.to_thread(get_StockPrices, ticker, minPeriod, maxPeriod)

async def get_fact_async(cik, ticker):

    compFacts_th, filings_th = await asyncio.gather(
        async_get_companyfacts(cik), async_get_SEC_filings(cik, ticker) #, async_get_StockPrices(ticker)
    )

    resDic = {'SEC_filings':filings_th, 'company_facts':compFacts_th}
    
    return resDic

async def get_prices_async(ticker, minPeriod, maxPeriod):

    try:
        prices = await asyncio.gather(
            async_get_StockPrices(ticker, minPeriod, maxPeriod) 
        )
        prices_dic = {"stock_prices" : prices[0]}
    except Exception as e:
        print("Error while proccesing", ticker)
        print(e)
        traceback.print_exc()
        prices_dic = {"stock_prices" : "error"}
        
    return prices_dic


def get_fact_dataframes_dict(cik, ticker):
    start = time.time()
    res = asyncio.run(get_fact_async(cik, ticker))
    
    print(cik, ticker, 'loaded in', time.time() - start)

    return res

def get_prices_dataframe(ticker, minPeriod, maxPeriod):
    start = time.time()
    res = asyncio.run(get_prices_async(ticker, minPeriod, maxPeriod))
    
    print(ticker, 'loaded in', time.time() - start)

    return res

    
def get_batch_sec(pool, ticketList, start_step, end_step):
    ticker = list(ticketList[start_step:end_step,0])
    ciks = list(ticketList[start_step:end_step,1])
    batch = pool.starmap(get_fact_dataframes_dict, zip(ciks, ticker))
    
    return batch

def unpact_batch_data(wholeDataInBatch):
    fillings_concat = pd.DataFrame()
    facts_concat = pd.DataFrame()

    for dic in wholeDataInBatch:
        fillings_concat = pd.concat([dic['SEC_filings'],fillings_concat ], axis = 0)
        facts_concat = pd.concat([dic['company_facts'],fillings_concat ], axis = 0)
    
    sec_data = {'SEC_filings' : fillings_concat, 'company_facts':facts_concat}
    
    return sec_data


def save_sec_batch_delta(sparkClass:sparkDelta,wholeDataInBatch):
    wholeDataInBatch = list(np.array(wholeDataInBatch).flatten())
    sec_data = unpact_batch_data(wholeDataInBatch)
    filings, companyfacts = sec_data['SEC_filings'], sec_data['company_facts']
    sparkClass.save_partition_DeltaTable(filings, 'SEC_filings', 'ticker','append')
    sparkClass.save_partition_DeltaTable(companyfacts, 'company_facts','accn', 'append')
    
    
def unpack_batch_yahoo(wholeDataInBatch):
    prices = pd.DataFrame()
    for element in wholeDataInBatch:
        if type(element['stock_prices']) != str:
            prices = pd.concat([element['stock_prices'], prices], axis = 0)

    return prices


def save_yahoo_price_batch_delta(sparkClass:sparkDelta,wholeDataInBatch):
    wholeDataInBatch = list(np.array(wholeDataInBatch).flatten())
    price_data = unpack_batch_yahoo(wholeDataInBatch)
    sparkClass.save_partition_DeltaTable(price_data, 'stock_prices', 'ticker','append')


def get_spy500_batch_sec(sparkClass:sparkDelta, ticketList, batchSize = 16):
    
    pool = multiprocessing.Pool(batchSize)
    
    batch_control = 1
    start_step = 0
    for end_step in range(batchSize, len(ticketList), batchSize):
        batch = [get_batch_sec(pool, ticketList, start_step, end_step)]
        start_step = end_step
        save_sec_batch_delta(sparkClass, batch)
        print('Batch', batch_control, "loaded")
        batch_control +=1
        
    if start_step != len(ticketList):
        batch = [get_batch_sec(pool, ticketList, start_step, len(ticketList))]
        save_sec_batch_delta(sparkClass, batch)
        print('Batch', batch_control, "loaded")
        print("Process has compleded")
        
    pool.close()
    pool.join()
    print("Data has loaded")
    print("-------------- Loaded tickers --------------")
    print(ticketList)
    
def get_yahoo_batch(pool, loadedTickers, start_step, end_step):
    ticker = list(loadedTickers[start_step:end_step,0])
    maxperiod = list(loadedTickers[start_step:end_step,1])
    minperiod = list(loadedTickers[start_step:end_step,2])
    batch = pool.starmap(get_prices_dataframe, zip(ticker, minperiod, maxperiod))

    return batch
    
def load_yahoo_stock_price(sparkClass:sparkDelta, batchSize = 6):
    loadedTickers = sparkClass.get_min_max_fillings_date_filter_ticker()
    try:
        loaded_tickers = sparkClass.get_spark_dataframe('stock_prices')
        loaded_tickers = loaded_tickers.select(col('ticker')).distinct().toPandas()
        loaded_tickers_inPrices = list(loaded_tickers.to_numpy().flatten())
    except:
        loaded_tickers_inPrices = []
    
    loadedTickers = loadedTickers[(~loadedTickers['ticker'].isin(loaded_tickers_inPrices))]
    loadedTickers = loadedTickers.to_numpy()

    pool = multiprocessing.Pool(batchSize)
    batch_control = 1
    start_step = 0
    for end_step in range(batchSize, len(loadedTickers), batchSize):  
        batch = [get_yahoo_batch(pool, loadedTickers,start_step,end_step)]
        time.sleep(2)

        save_yahoo_price_batch_delta(sparkClass, batch)
        print('Batch', batch_control, "loaded")
        start_step = end_step
        batch_control +=1
        
    if start_step != len(loadedTickers):
        batch = [get_yahoo_batch(pool,loadedTickers, start_step,len(loadedTickers))]
        save_yahoo_price_batch_delta(sparkClass, batch)
        print('Batch', batch_control, "loaded")
        print("Process is completed")
        
    pool.close()
    pool.join()
    print("Data loaded")
    print("-------------- Loaded tickers --------------")
    print(loadedTickers)

        
def load_ticket_to_delta(spark: sparkDelta, batchSize = 16, tickers = None, load_stockPrice = True):
    spy500 = spark.get_spark_dataframe(filename='spy500', toDataframe = True)
    try:
        loaded_tickers = spark.get_spark_dataframe('SEC_filings')
        loaded_tickers = loaded_tickers.select(col('ticker')).distinct().toPandas()
        loaded_tickers = list(loaded_tickers.to_numpy().flatten())
    except:
        loaded_tickers = []
    
    if tickers:
        spy500 = spy500[(spy500['Symbol'].isin(tickers)) & (~spy500['Symbol'].isin(loaded_tickers))]
    
    spy500 = spy500[['Symbol', 'CIK']].to_numpy()
    get_spy500_batch_sec(spark, spy500, batchSize)


def initialize_spy_ticker_sec():
    spark = sparkDelta()
    write_spy500(spark)
    write_ticker_sec(spark)
    spark.sparkStop()

if __name__=="__main__":
    sparkClass = sparkDelta()
    #initialize_spy_ticker_sec()
    #ingest_Facts_Fillings(sparkClass,'NVDA')
    #load_spy500(sparkClass, 190, 190)
    tickers = ['MMM', 'AOS', 'ABT', 'ABBV', 'ACN', 'ADBE', 'AMD', 'AES', 'AFL',
       'A', 'APD', 'ABNB', 'AKAM', 'ALB', 'ARE', 'ALGN', 'ALLE', 'LNT',
       'ALL', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMCR', 'AEE', 'AAL', 'AEP',
       'AXP', 'AIG', 'AMT', 'AWK', 'AMP', 'AME', 'AMGN', 'APH', 'ADI',
       'ANSS', 'AON', 'APA', 'AAPL', 'AMAT', 'APTV', 'ACGL', 'ADM',
       'ANET', 'AJG', 'AIZ', 'T', 'ATO', 'ADSK', 'ADP', 'AZO', 'AVB',
       'AVY', 'AXON', 'BKR', 'BALL', 'BAC', 'BK', 'BBWI', 'BAX', 'BDX',
       'BRK.B', 'BBY', 'BIO', 'TECH', 'BIIB', 'BLK', 'BX', 'BA', 'BKNG',
       'BWA', 'BSX', 'BMY', 'AVGO', 'BR', 'BRO', 'BF.B', 'BLDR', 'BG',
       'BXP', 'CHRW', 'CDNS', 'CZR', 'CPT', 'CPB', 'COF', 'CAH', 'KMX']
    #get_fact_dataframes_dict('CIK0001045810', 'NVDA')
    #get_spy500_batch_sec(batchSize=3)
    #load_ticket_to_delta(sparkClass, batchSize=16, tickers = tickers)
    load_yahoo_stock_price(sparkClass)
    
    sparkClass.sparkStop()
    #initialize_spy_ticker_sec()
    pass