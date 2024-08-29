# DeltaStock 🚀

**DeltaStock** is a data processing project that utilizes Apache Spark and Delta Lake to handle and merge stock price data with financial records. By integrating with Azure Blob Storage, it provides scalable and secure storage for large datasets. The project aims to offer an integrated platform for analyzing the relationship between stock prices and company financial performance. It leverages advanced big data technologies to facilitate the addition of new data sources and enable querying with Spark SQL and PySpark, streamlining data analysis and insights generation.

## Features

- **Delta Lake Integration**: Efficient and ACID-compliant data storage with Delta Lake.
- **Azure Blob Storage**: Seamless interaction with Azure Blob Storage for data storage and retrieval.
- **Data Merging**: Includes scripts for merging stock prices and financial records using Spark.

## Data Sources

- **Stock Price Data**: Historical stock prices sourced from Yahoo Finance.
- **Financial Data**: Company financial records sourced from SEC EDGAR, including items like assets, revenue, etc.
- **S&P 500 Stocks**: Table that contains tickers and CIK numbers of listed companies.


### Example Query

An example method for querying the data can be found in the `notebook` folder, specifically in the `method.ipynb` file. Below is a sample query using Spark SQL:

```python
spark.spark.sql("""
                SELECT p.ticker, fintype, val, adjclose, fileURL, cf.yearMonthDay FROM company_facts AS cf
                JOIN stock_prices as p ON (cf.ticker = p.ticker and cf.yearMonthDay = p.yearMonthDay)
                JOIN SEC_filings as fil ON (fil.ticker = cf.ticker and fil.accessionNumber = cf.accessionNumber)
                WHERE cf.ticker = 'BXP'
                """).show(5)

```

This query joins the `company_facts`, `stock_prices`, and `SEC_filings` tables to retrieve financial information, stock prices, and filing URLs for the ticker `BXP`.

The output of the query would look like this:

```plaintext
+------+------------+------+-----------------+--------------------+------------+
|ticker|     fintype|   val|         adjclose|             fileURL|yearMonthDay|
+------+------------+------+-----------------+--------------------+------------+
|   BXP|LineOfCredit|   0.0|85.38506317138672|https://www.sec.g...|    20210331|
|   BXP|LineOfCredit|   0.0|97.44332122802734|https://www.sec.g...|    20210630|
|   BXP|LineOfCredit|   0.0|92.94353485107422|https://www.sec.g...|    20210930|
|   BXP|LineOfCredit|1.45E8|99.64728546142578|https://www.sec.g...|    20211231|
|   BXP|LineOfCredit|1.45E8|99.64728546142578|https://www.sec.g...|    20211231|
+------+------------+------+-----------------+--------------------+------------+
only showing top 5 rows
```


## Data Ingestion

The data ingestion process for the DeltaStock project involves fetching stock price data and financial records for a specified list of tickers. This process is executed using the `load_initialize_tickers` function, which can be found in the `pipeline.py` file. The function allows for scalable batch processing, ensuring efficient handling of large datasets.


### Current process

The data ingestion process in the DeltaStock project involves two main steps:

1. **SEC Filings Data**: The financial records from the SEC EDGAR database are downloaded separately. This ensures that the necessary financial data is available before processing stock prices.

2. **Stock Prices from Yahoo Finance**: Stock price data is retrieved from Yahoo Finance, but it only includes the time range that corresponds to the minimum and maximum dates present in the SEC filings table. This selective loading helps in reducing unnecessary data retrieval and focuses the analysis on relevant periods.

### Batch Loading

Data is loaded in batches for efficient processing. Below is an example of the batch loading process:

```plaintext
COF loaded in 1.2694916725158691 seconds
CDNS loaded in 1.4411845207214355 seconds
BXP loaded in 1.7666106224060059 seconds
CAH loaded in 2.0857300758361816 seconds
CPB loaded in 2.1732027530670166 seconds
CHRW loaded in 2.2683205604553223 seconds
Batch 1 loaded
CZR loaded in 1.3973908424377441 seconds
KMX loaded in 1.7747790813446045 seconds
CPT loaded in 1.9990382194519043 seconds
Batch 2 loaded
```

#### Ticker List
Currently data only can be downoaded using spy500 companies, you can provide it in filowing form, or just use full load funtion.

```python
tickers = ['MMM', 'AOS', 'ABT', 'ABBV', 'ACN', 'ADBE', 'AMD', 'AES', 'AFL', 'A' ...]
```

#### Executing the Ingestion

To initiate the data ingestion, you can execute the following function within the `pipeline.py` file:

```python
load_initialize_tickers(tickers, spy_loaded=True, batch=16)
```
