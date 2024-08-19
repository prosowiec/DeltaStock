from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, StringType, TimestampType, LongType

fact_schema = StructType([
    StructField("finType", StringType(), True),
    StructField("label", StringType(), True),
    StructField("description", StringType(), True),
    StructField("end", StringType(), True),
    StructField("val", DoubleType(), True),
    StructField("accn", StringType(), True),
    StructField("fy", IntegerType(), True),
    StructField("fp", StringType(), True),
    StructField("form", StringType(), True),
    StructField("filed", StringType(), True),
    StructField("frame", StringType(), True),
    StructField("endFormat", StringType(), True),
    StructField("start", StringType(), True),
    StructField("startFormat", StringType(), True),
    StructField("diffDate", StringType(), True),
    StructField("monthWindow", StringType(), True),
    StructField("yearMonthDay", StringType(), True)
])


filing_schema = StructType([
    StructField("accessionNumber", StringType(), True),
    StructField("filingDate", StringType(), True),
    StructField("reportDate", StringType(), True),
    StructField("acceptanceDateTime", StringType(), True),
    StructField("act", StringType(), True),
    StructField("form", StringType(), True),
    StructField("fileNumber", StringType(), True),
    StructField("filmNumber", StringType(), True),
    StructField("items", StringType(), True),
    StructField("size", LongType(), True),
    StructField("isXBRL", IntegerType(), True),
    StructField("isInlineXBRL", IntegerType(), True),
    StructField("primaryDocument", StringType(), True),
    StructField("primaryDocDescription", StringType(), True),
    StructField("fileURL", StringType(), True),
    StructField("yearMonthDay", StringType(), True),
    StructField("ticker", StringType(), True)
])

wiki_schema = StructType([
    StructField("Symbol", StringType(), True),
    StructField("Security", StringType(), True),
    StructField("GICS_Sector", StringType(), True),
    StructField("GICS_Sub_Industry", StringType(), True),
    StructField("Headquarters_Location", StringType(), True),
    StructField("Date_added", StringType(), True),
    StructField("CIK", StringType(), True),
    StructField("Founded", StringType(), True)
])

prices_schema = StructType([
    StructField("adjClose", DoubleType(), True),
    StructField("date", StringType(), True),
    StructField("ticker", StringType(), True),
    StructField("Quarter", StringType(), True),
    StructField("meanADJclose", DoubleType(), True)
])

sec_ticker_schema = StructType([
    StructField("cik_str", IntegerType(), True),
    StructField("ticker", StringType(), True),
    StructField("title", StringType(), True),
    StructField("cik10D", StringType(), True)
])


schemaSelect = {
    'company_facts' : fact_schema,
    'SEC_filings' : filing_schema,
    'spy500' : wiki_schema,
    'prices_schema' : prices_schema,
    'ticker-sec' : sec_ticker_schema
}

"""
ticker-sec
spy500
SEC_filings
company_facts
"""