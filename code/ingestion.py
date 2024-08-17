from spark_deltalake.spark_operations import sparkDelta
from source.scraper import generate_CIK_TICKER, get_spy500_formWiki

def write_ticker_sec(sparkclass, filename = 'ticker-sec'):
    sec_ticker = generate_CIK_TICKER()
    sparkclass.save_small_DeltaTable(sec_ticker, filename)
    print("Saved table {}".format(filename))
    
def write_spy500(sparkclass, filename = 'spy500'):
    wikispy = get_spy500_formWiki()
    sparkclass.save_small_DeltaTable(wikispy, filename)
    print("Saved table {}".format(filename))
    
def append_companyFacts(ticker):
    pass


if __name__=="__main__":
    spark = sparkDelta()
    write_spy500(spark)
    write_ticker_sec(spark)
    spark.sparkStop()