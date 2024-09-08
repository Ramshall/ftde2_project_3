# Project 3: Batch Processing Using Airflow, Spark, and TiDB

## Project Overview
This project demonstrates a batch processing pipeline that integrates Apache Airflow, Apache Spark, PostgreSQL, and TiDB. The pipeline orchestrates the ETL process of rental data from a PostgreSQL database, transforming the data using Spark and storing the results in Parquet and TiDB formats. An API is then used to expose the processed data.

## Objectives
Objectives:
* Orchestrate and schedule data processing tasks using Airflow.
* Extract and transform rental data from PostgreSQL using Spark.
* Store processed data in Parquet files to TiDB.
* Expose the transformed data via an API for external access

## Tools:
* Apache Airflow: Orchestration and scheduling.
* Apache Spark: Distributed data processing.
* PostgreSQL: Source of rental data.
* TiDB: For storing processed data.
* Pandas: Used to manipulate and interact with the data.
* Postman: This is for testing and interacting with the API.

## Prerequisites
* Docker and Docker Compose are installed on your machine.
* PostgreSQL and TiDB instances running.
* Python 3.8+ environment set up.
* pyspark==3.5.2
* psycopg2-binary==2.9.9
* mysql-connector-python==9.0.0
* pandas

## Project Workflow:

![Screenshot 2024-09-08 144942](https://github.com/user-attachments/assets/df636065-8d3c-43e7-bec4-bb00e70de7f2)

## Architecture Overview
The pipeline processes rental data stored in a PostgreSQL database. The data is extracted and transformed using Apache Spark and then stored in Parquet format as well as in TiDB. The processed data can be accessed through an API, with the API endpoints tested using Postman.

Data Flow:
1. PostgreSQL
   * Source of raw rental data,
     
2. Apache Spark
   * Performs ETL on the PostgreSQL data.
   * Transforms and stores data into Parquet files.
     
3. Pandas
   * Further manipulates the transformed data.
   * Prepares the data for insertion into TiDB.
 
4. TiDB
   * Stores the final processed data
     
5. API
   * Exposes the data stored in TiDB.
   * Accessible through Postman for querying the processed data

The Orchestrator

![Screenshot 2024-09-08 134527](https://github.com/user-attachments/assets/d120a859-ea65-4ce8-8a68-8fccc5084a39)

The DAG d_1_batch_processing_spark performs two main tasks:

* Top Countries Analysis
* Total Films per Category Analysis

The DAG runs daily at 23:00 UTC, executing a start task, followed by parallel processing of Top Countries and Total Films per Category analyses, before concluding with an end task.

Ensure the following Airflow connections are set up:
* postgres_conn: Connection to PostgreSQL database
* tidb_conn: Connection to TiDB database

Also, set up the Airflow Variable:
* spark_jars_packages: Required Spark JAR packages

## Results
The transformed rental data is stored in Parquet files and TiDB and the API exposes the processed data for external access, and Postman can be used to test it.
