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
                """).show()

```

This query joins the `company_facts`, `stock_prices`, and `SEC_filings` tables to retrieve financial information, stock prices, and filing URLs for the ticker `BXP`.

The output of the query would look like this:

```plaintext
+------+--------------------+------+------------------+--------------------+------------+
|ticker|             fintype|   val|          adjclose|             fileURL|yearMonthDay|
+------+--------------------+------+------------------+--------------------+------------+
|   BXP|        LineOfCredit|   0.0| 85.38506317138672|https://www.sec.g...|    20210331|
|   BXP|        LineOfCredit|   0.0| 97.44332122802734|https://www.sec.g...|    20210630|
|   BXP|        LineOfCredit|   0.0| 92.94353485107422|https://www.sec.g...|    20210930|
|   BXP|        LineOfCredit|1.45E8| 99.64728546142578|https://www.sec.g...|    20211231|
|   BXP|        LineOfCredit|1.45E8| 99.64728546142578|https://www.sec.g...|    20211231|
|   BXP|        LineOfCredit|1.45E8| 99.64728546142578|https://www.sec.g...|    20211231|
|   BXP|        LineOfCredit|1.45E8| 99.64728546142578|https://www.sec.g...|    20211231|
|   BXP|        LineOfCredit|1.45E8| 99.64728546142578|https://www.sec.g...|    20211231|
|   BXP|        LineOfCredit|2.55E8|112.26079559326172|https://www.sec.g...|    20220331|
|   BXP|        LineOfCredit|1.65E8| 78.39462280273438|https://www.sec.g...|    20220630|
|   BXP|        LineOfCredit| 3.4E8|   66.908447265625|https://www.sec.g...|    20220930|
|   BXP|        LineOfCredit|   0.0| 49.94557189941406|https://www.sec.g...|    20230331|
|   BXP|        LineOfCredit|   0.0| 54.08084487915039|https://www.sec.g...|    20230630|
|   BXP|LineOfCreditFacil...|0.0015|102.87355041503906|https://www.sec.g...|    20210615|
|   BXP|LineOfCreditFacil...|0.0015|102.87355041503906|https://www.sec.g...|    20210615|
|   BXP|LineOfCreditFacil...|0.0015| 99.64728546142578|https://www.sec.g...|    20211231|
|   BXP|LineOfCreditFacil...|0.0015| 54.08084487915039|https://www.sec.g...|    20230630|
|   BXP|LineOfCreditFacil...| 1.0E9| 89.93573760986328|https://www.sec.g...|    20141231|
|   BXP|LineOfCreditFacil...| 1.0E9| 91.87318420410156|https://www.sec.g...|    20151231|
|   BXP|LineOfCreditFacil...| 1.5E9|102.87355041503906|https://www.sec.g...|    20210615|
+------+--------------------+------+------------------+--------------------+------------+
only showing top 20 rows
```
