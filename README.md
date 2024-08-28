# 🚀 DeltaStock 🚀

**DeltaStock** is a data processing project that utilizes Apache Spark and Delta Lake to handle and merge stock price data with financial records. By integrating with Azure Blob Storage, it provides scalable and secure storage for large datasets. The project aims to offer an integrated platform for analyzing the relationship between stock prices and company financial performance. It leverages advanced big data technologies to facilitate the addition of new data sources and enable querying with Spark SQL and PySpark, streamlining data analysis and insights generation.

## Features

- **Delta Lake Integration**: Efficient and ACID-compliant data storage with Delta Lake.
- **Azure Blob Storage**: Seamless interaction with Azure Blob Storage for data storage and retrieval.
- **Data Merging**: Includes scripts for merging stock prices and financial records using Spark.

## Data Sources

- **Stock Price Data**: Historical stock prices sourced from Yahoo Finance.
- **Financial Data**: Company financial records sourced from SEC EDGAR, including items like assets, revenue, etc.
- **S&P 500 Stocks**: Table that contains tickers and CIK numbers of listed companies.
