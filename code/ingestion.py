from spark_deltalake.spark_operations import sparkDelta
from source.scraper import generate_CIK_TICKER, get_spy500_formWiki, fillTo10D, get_SEC_filings, get_companyfacts

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

def ingestFact(ticker):
    sparkClass = sparkDelta()
    cik = get_cik_by_ticker(sparkClass, ticker)
    #append_company_facts(sparkClass, ticker, cik)
    append_SEC_filings(sparkClass, ticker, cik)
    
    sparkClass.sparkStop()

def initialize_spy_ticker_sec():
    spark = sparkDelta()
    write_spy500(spark)
    write_ticker_sec(spark)
    spark.sparkStop()

if __name__=="__main__":
    ingestFact('AAPL')
    #initialize_spy_ticker_sec()