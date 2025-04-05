# Overview
This project implements an **end-to-end ETL pipeline** that transforms raw e-commerce data into an analytical data warehouse with complete historical tracking.

## Key Components
### Source Data (OLTP)
The raw data source is initially stored in an Excel file. To simulate a real-world scenario, **this data will be inserted into an OLTP (Online Transaction Processing) database**, which is designed for transactional processing and real-time updates—commonly used in industries such as banking and e-commerce.

The OLTP database consists of the following five tables:

1. users

2. transactions

3. membership_purchases

4. user_activity

5. merchant_discount_rate

**SQLite is chosen as the OLTP database due to its simplicity and portability**, making it a suitable choice for this small-scale project.

### Data Warehouse (OLAP)
The goal of this project is to **analyze user profiling and transaction summaries**, which require maintaining historical records. To achieve this, **we will implement Slowly Changing Dimension (SCD) Type 2**, a technique that tracks historical changes by adding columns such `aseffective_start_date`, `effective_end_date`, and `is_active`.

The data warehouse consists of the following tables:

1. user_profiling

2. transaction_summary

Similar to the OLTP database, **we use DuckDB as the OLAP (Online Analytical Processing) database due to its simplicity and portability**.

# Tech Stack
1. Docker: Use for containerize whole ETL pipeline, so it can run anywhere without worrying about the environment.

2. Python: Main programming language used to do the ETL.

3. Crontab: A minimalist job scheduler used to run the ETL process daily at 00:00 UTC. Crontab is chosen for its simplicity and lightweight nature, making it a suitable alternative to more complex standalone orchestrators like Airflow or Prefect.

4. SQLite: OLTP database to store transactional processing.

5. DuckDB: OLAP database to use for analytical processing.

# Installation
1. Make sure you have installed Docker in your system. Follow [this guidline](https://docs.docker.com/engine/install/) to install it.

2. Run docker compose:
    ```bash
    docker compose up -d
    ```

3. It automatically run the job every day at 00:00 UTC. The generated report will be saved in `./reports`, In order to see the logs of ETL job simply run:
    ```bash
    docker logs etl-ecommerce-sprout
    ```
    Example output:
    ```bash
    [2025-04-04 05:34:01] Attempt 1 running main.py with --date 2025-04-04
    ETL is starting.
    ETL is finished.
    [2025-04-04 05:34:01] Success!
    ```

# Example
The example output of generated reports can be seen in the directory `./reports/2025-04-04.xlsx`.