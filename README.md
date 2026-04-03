# RetailFlow — End-to-End Data Engineering Pipeline

An end-to-end data pipeline built with Apache Airflow, dbt, PostgreSQL, and Metabase using the Olist Brazilian E-commerce dataset.

## Stack
| Layer | Tool |
|---|---|
| Ingestion | Apache Airflow |
| Storage | PostgreSQL |
| Transformation | dbt |
| Visualization | Metabase |
| Infrastructure | Docker Compose |

## Dashboard
![RetailFlow Dashboard](screenshots/dashboard.png)

## Architecture
Raw CSVs → Airflow DAG → PostgreSQL → dbt (staging → facts → marts) → Metabase

## Dataset
[Olist Brazilian E-commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 100k+ orders