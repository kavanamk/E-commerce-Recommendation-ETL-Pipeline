
# E-Commerce Recommendation ETL Pipeline

PySpark • Airflow • Data Pipeline Project

## Overview

This project demonstrates a complete data pipeline built with PySpark and Airflow. The goal is to take raw Amazon video-game review data, clean it, organize it, and produce useful daily product insights that can power reporting dashboards or recommendation models.

The pipeline follows a clear structure used by modern data engineering teams:
**Raw Data → Clean Data → Business Metrics**

---

## How the Pipeline Works
<img width="1000" height="1892" alt="image" src="https://github.com/user-attachments/assets/34a1dee5-a491-4cfc-8f85-e9dc8894860f" />

```
Raw JSON Reviews
      │
      ▼
ingestion.py          (load raw data)
      │
      ▼
transform.py          (clean and standardize data)
      │
      ▼
product_daily_metrics.py   (create daily product insights)
```

Airflow schedules and runs each step in order so the pipeline is reliable, repeatable, and easy to manage.

---

## What the Pipeline Produces

The final output is a set of **daily product metrics** such as:

* average rating per product
* number of reviews
* rating distribution (1–5 stars)
* first and last review dates

These insights can be used for:

* product ranking
* recommendation systems
* business dashboards
* trend analysis

---

## Technologies Used

* **PySpark** for large-scale data processing
* **Apache Airflow** for orchestration and scheduling
* **Parquet** files for efficient data storage
* **Python** for pipeline logic

---

## Project Structure

```
airflow/dags/                     Airflow workflow
spark_jobs/                       PySpark ETL scripts
data/                             Raw input data
processed/                        Clean and aggregated outputs
```

---

