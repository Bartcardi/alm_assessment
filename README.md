# ABN Amro Data Warehouse Assessment Project
This repository contains the code and resources for a Data Lake assessment project. To ensure a consistent and reproducible development environment, it's recommended to open this project in a Dev Container using Visual Studio Code (VSCode). This will automatically set up all the necessary dependencies and configurations.

# Getting Started with Dev Containers in VSCode
1. **Install Docker:** Ensure you have Docker installed on your system. You can download it from https://www.docker.com/get-started.
2. **Install Remote - Containers Extension:** In VSCode, install the "Remote - Containers" extension from the marketplace.
3. **Open in Container:** Open the project folder in VSCode. You should see a green button in the lower-left corner of VSCode. Click on it and select "Reopen in Container". This will build and launch the Dev Container based on the provided configuration.
# Project Overview
This project demonstrates the following key aspects of a data lake architecture:

* **Data Ingestion:**
    * Importing raw data from CSV and Parquet sources.
    * Defining and applying schemas for data consistency.
    * Loading data into a "bronze" layer in the data lake.
* **PySpark and Delta Lake:**
    * Utilizing PySpark for efficient data processing and transformation.
    * Leveraging Delta Lake's features for data reliability (ACID transactions, schema evolution) and optimized performance.
* **Testing with Pytest:**
    * Implementing Pytest unit tests to validate data integrity and correctness.
    * Ensuring code quality and reliability through testing.

# Project Structure
* `data/Source Files`: Contains sample source data files (CSV, Parquet).
* `data/Lookup Files`: Contains the lookup files in csv.
* `conftest.py`: Pytest configuration file with fixtures for a shared SparkSession.
* `utils.py`: (Optional) This module could contain utility functions for data processing and validation but now only contains a handle to the spark session.
* `init_bronze.py`: The main script for data ingestion in bronze.
* `ETL.py`: The main script for data cleanup and transformations.
* `test_*.py`: Pytest test files for unit testing your data processing functions.

# Running the Code
Once the Dev Container is up and running, you can execute the following commands in the VSCode integrated terminal:

Install the project package:

```Bash
pip install -e .
```

Run initialisation of bronze tables:

```bash
python3 src/init_bronze_db.py
```

Run the ETL processes:

```bash
python3 src/ETL.py
```

or press the run button in VSCode for the respective files. 

Run the tests:

```Bash
pytest
```

# Data Flow
1. Raw Data: Source data is located in the `data/Source Files` and `data/Lookup Files` directory.
2. Bronze Layer: Data is ingested as-is into Delta tables in the "bronze" database.
3. Silver Layer: Data is enriched with procedures in `ETL.py` 

# Assumptions

1. There are invalid dates in `ClientSince`. In order to correct data quality issues we would normally have to talk a data officer for instance. Now I have just stripped one digit to demonstrate one possible solution for input error handling. 
2. `LoanCheck` and `DiscountCheck` have `NA` as default value in the mapping excel. Therefore, I have put an empty string in the corresponding rows where this attribute is missing (instead of `None`) 
3. Some values for `ClientNumber` which are in the data sources are not in the lookup table and are therefore marked `None` in `ClientSecuredIND` and are not considered in the secured amount transformation. 

# Answers

1. Value of SecuredAmount for Client_Number 6991

```python
spark.sql("SELECT SecuredAmount from silver.Final where ClientNumber==6991").show()
```

```shell
+-------------+
|SecuredAmount|
+-------------+
|          0.0|
+-------------+
```

2. Number of records where EnterprizeSize is M

```python
spark.sql("SELECT count(*) as NumberMSizedEnterprizes from silver.Final where EnterprizeSize=='M'").show()
```

```shell
+-----------------------+
|NumberMSizedEnterprizes|
+-----------------------+
|                      6|
+-----------------------+
```

3. Total Sum of AmountEUR for all records for Source2

```python
spark.sql("SELECT sum(AmountEUR) as TotalSumAmountEURSource2 from silver.Final where SourceSystem=='Source2'").show()
```

```shell
+------------------------+
|TotalSumAmountEURSource2|
+------------------------+
|              1800631.68|
+------------------------+
```
4. Total Sum of OriginalAmount for all the records with ClientSecuredIND false

```python
spark.sql("SELECT sum(OriginalAmount) as TotalSumOriginalAmountClientSecuredFalse from silver.Final where ClientSecuredIND==false").show()
```

```shell
+----------------------------------------+
|TotalSumOriginalAmountClientSecuredFalse|
+----------------------------------------+
|                               297518.47|
+----------------------------------------+
```
