# ETL Pipeline Using Pandas - README

## Overview

This README outlines the implementation of an ETL (Extract, Transform, Load) pipeline designed to process and analyze data for a streaming service company. The pipeline uses Python with Pandas and is structured to fulfill the requirements of a three-phase project. The phases include data handling (with potential updates or deletions from the vendor), querying for business insights, and a detailed architecture plan for production deployment.

ETL Process using Pandas
This project demonstrates an Extract, Transform, Load (ETL) pipeline using Python and the Pandas library. The ETL process extracts data from various sources, transforms it, and loads it into a production-ready database. It follows a structured approach to handle data from different formats and clean it for analysis.

Table of Contents
Prerequisites
Project Structure
Usage
ETL Process
Questions and SQL Queries
Contributing
Prerequisites
Before running the ETL process, make sure you have the following prerequisites:

Python (3.6+)
Pandas library (pip install pandas)
PostgreSQL database (for the final data storage)
Data files (CSV and JSON) in the data folder

Project Structure
The project structure is organized as follows:

ETL_Pandas_Project/
│
├── data/ (folder containing source data files)
│   ├── authors.json
│   ├── books.json
│   ├── movies.csv
│   ├── reviews.json
│   └── users.csv
│
├── output/ (folder for cleaned and partitioned data)
│
├── ETL_pandas.py (main ETL script)
└── README.md

Usage
To execute the ETL process, run the ETL_pandas.py script. Make sure to customize the script according to your specific data sources and requirements.

python ETL_pandas.py

The script will perform the following steps:

-Load data from CSV and JSON files.
-Clean and preprocess the data (removing duplicates, handling missing values, and more).
-Flatten nested JSON structures (if present).
-Remove special characters from data.
-Save the cleaned data into separate CSV files in the output folder.

ETL Process
The ETL process consists of the following steps:

-Extraction: Data is loaded from various CSV and JSON files located in the data folder.

-Transformation: The data is cleaned, processed, and transformed to ensure consistency and quality. This includes handling duplicates, missing values, and flattening nested JSON structures.

-Loading: The cleaned data is saved as separate CSV files in the output folder, making it ready for loading into a production database.

## Phase 1: Data Processing Pipeline

### Extract

- **Data Sources**: Data is sourced from JSON and CSV files, which includes authors, books, reviews, users, movies, and streaming data.
- **Extraction Method**: Python's Pandas library is used to load data from these files. Functions `load_csv` and `load_json` are implemented for this purpose.

### Transform

- **Cleaning Data**: Utilize `clean_data` function to remove duplicates and handle missing values. For JSON data, `flatten_json` function is employed to transform nested structures into a flat format.
- **Handling Updates and Deletions**: The pipeline is designed to handle updates and deletions. A strategy is implemented to compare newly extracted data with existing data to identify changes.
- **Data Transformation**: Necessary transformations are applied to data to meet the business requirements and to facilitate efficient querying in the subsequent phase.

### Load

- **Loading to Database**: The transformed data is loaded into a PostgreSQL database hosted on AWS RDS for production use. This step ensures that data is stored in a structured and queryable format.

## Phase 2: Data Querying

SQL queries are used to derive insights from the processed data. The queries address specific business questions such as:

1. Percentage of streamed movies based on books.
SELECT
  ROUND(COUNT(*) FILTER (WHERE m.based_on_book = true) * 100.0 / COUNT(*), 2) AS percentage_based_on_books
FROM
  streams s
JOIN movies m ON s.movie_id = m.id
WHERE
  m.based_on_book IS NOT NULL;


2. Number of users potentially affected by a system outage.

SELECT
  COUNT(DISTINCT user_id) AS affected_users
FROM
  streams
WHERE
  start_at <= 'YYYY-MM-DD HH:MM:SS' -- Outage start time
  AND end_at >= 'YYYY-MM-DD HH:MM:SS'; -- Outage end time

3. Count of movies based on books written by Singaporean authors streamed in the month.

SELECT
  COUNT(DISTINCT s.movie_id) AS singaporean_authors_movie_count
FROM
  streams s
JOIN movies m ON s.movie_id = m.id
JOIN books b ON m.book_id = b.id
JOIN authors a ON b.author_id = a.id
WHERE
  a.nationality = 'Singaporean'
  AND s.stream_date BETWEEN 'YYYY-MM-01' AND 'YYYY-MM-31';


4. Average streaming duration.

SELECT
  AVG(EXTRACT(EPOCH FROM (end_at - start_at))/60) AS avg_streaming_duration_minutes
FROM
  streams;


5. Median streaming size in gigabytes.

SELECT
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY size_mb / 1024) AS median_streaming_size_gb
FROM
  streams;


6. Users who watched at least 50% of any movie in the last week of the month.

SELECT
  COUNT(DISTINCT user_id) AS users_watched_at_least_half
FROM
  streams s
JOIN movies m ON s.movie_id = m.id
WHERE
  EXTRACT(EPOCH FROM (s.end_at - s.start_at)) >= 0.5 * m.duration_mins * 60
  AND s.start_at >= date_trunc('month', current_date) + interval '3 weeks'
  AND s.start_at < date_trunc('month', current_date) + interval '1 month';


Each query is crafted to extract precise information from the PostgreSQL database, making use of its efficient querying capabilities.

## Phase 3: Detailed Architecture for Production

In a production environment, the pipeline would be scaled and optimized. The following architecture and technologies are proposed:

- **Data Storage**: AWS S3 buckets for initial data storage, ensuring scalability and accessibility.
- **ETL Orchestration**: Apache Airflow for managing the ETL workflow, providing a robust and flexible way to schedule and monitor ETL jobs.
- **Data Processing**: Pandas for data processing, given its efficiency and ease of use in handling and transforming large datasets.
- **Database**: AWS RDS with PostgreSQL for reliable, scalable, and efficient data storage and retrieval.
- **Monitoring and Logging**: Integration of monitoring and logging tools within Airflow and AWS services to ensure pipeline reliability and to quickly address issues.
- **Security**: Implementation of security measures at each stage of the pipeline to protect data integrity and privacy.

The pipeline is designed with an emphasis on reliability, efficiency, and scalability, ensuring that it can handle large volumes of data and provide timely insights for business decision-making.
