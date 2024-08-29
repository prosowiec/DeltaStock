from spark_deltalake.spark_operations import sparkDelta
from ingestion import initialize_spy_ticker_sec, load_ticket_to_delta, load_yahoo_stock_price

def load_initialize_tickers(tickers, spy_loaded = False):
    sparkClass = sparkDelta()
    if not spy_loaded:
        initialize_spy_ticker_sec()
    load_ticket_to_delta(sparkClass, batchSize=16, tickers = tickers)
    load_yahoo_stock_price(sparkClass)
    sparkClass.sparkStop()
    
def load_initialize_full():
    sparkClass = sparkDelta()
    initialize_spy_ticker_sec()
    load_ticket_to_delta(sparkClass, batchSize=16, tickers = tickers)
    load_yahoo_stock_price(sparkClass)
    sparkClass.sparkStop()



if __name__=="__main__":
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

    load_initialize_tickers(tickers,True)