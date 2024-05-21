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
* `data/`: Contains sample source data files (CSV, Parquet).
* `conftest.py`: Pytest configuration file with fixtures for a shared SparkSession.
* `utils.py`: (Optional) This module could contain utility functions for data processing and validation.
* `main.py`: The main script for data ingestion and transformation.
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

or press the run button in VSCode

Run the tests:

```Bash
pytest
```

# Data Flow
1. Raw Data: Source data is located in the data/ directory.
2. Bronze Layer: Data is ingested as-is into Delta tables in the "bronze" database.