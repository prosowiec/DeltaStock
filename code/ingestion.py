from spark_deltalake.spark_operations import sparkDelta
from source.scraper import generate_CIK_TICKER, get_spy500_formWiki, \
    fillTo10D, get_SEC_filings, get_companyfacts, get_StockPrices
from tqdm import tqdm
import pandas as pd

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
    
def append_filings_bookmark_values(sparkClass : sparkDelta,ticker ,filingTableName = 'SEC_filings'):
    filling = sparkClass.get_spark_dataframe(sparkClass, filingTableName, createView=True)
    

    #sparkClass.save_partition_DeltaTable(prices, 'stock_prices', 'ticker','append')

    
def append_StockPrices(sparkClass : sparkDelta, ticker, cik = None):
    if not cik:
        cik = get_cik_by_ticker(sparkClass, ticker)
    prices = get_StockPrices(ticker, startSelect='2020-01-01')
    sparkClass.save_partition_DeltaTable(prices, 'stock_prices', 'ticker','append')

    

def append_company_facts(sparkClass : sparkDelta, ticker, cik = None):
    if not cik:
        cik = get_cik_by_ticker(sparkClass, ticker)
    companyfacts = get_companyfacts(cik)
    sparkClass.save_partition_DeltaTable(companyfacts, 'company_facts','accn', 'append')

def ingest_Facts_Fillings(sparkClass : sparkDelta, ticker, cik):
    append_company_facts(sparkClass, ticker, cik)
    append_SEC_filings(sparkClass, ticker, cik)
    
def load_spy500(sparkClass:sparkDelta, max_index = 2):
    ciks = sparkClass.get_spark_dataframe('spy500', toDataframe=True)
    for index, row in tqdm(ciks.iterrows()):
        if index == max_index and max_index >= 0:
            break
        
        ingest_Facts_Fillings(sparkClass, row['Symbol'], row['CIK'])

def initialize_spy_ticker_sec():
    spark = sparkDelta()
    write_spy500(spark)
    write_ticker_sec(spark)
    spark.sparkStop()

if __name__=="__main__":
    sparkClass = sparkDelta()
    #initialize_spy_ticker_sec()
    #ingest_Facts_Fillings(sparkClass,'NVDA')
    load_spy500(sparkClass)
    sparkClass.sparkStop()
    #initialize_spy_ticker_sec()
    pass