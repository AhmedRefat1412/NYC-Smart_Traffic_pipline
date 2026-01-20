# NYC Smart Traffic Pipeline

End-to-End Batch & Streaming Data Pipeline for Urban Traffic Analytics in New York City.

## Overview
This project implements a scalable data engineering platform that processes both
historical and real-time traffic-related data to analyze road incidents, weather impact,
and urban mobility patterns.

The solution follows a modern lakehouse-style architecture and is fully containerized
using Docker.

## Data Sources
**Batch Data**
- NYC 311 service requests (MongoDB)
- Traffic accidents data (CSV)
- Streets metadata
- Weather data

**Streaming Data**
- GitBike station status API (real-time)

## Pipeline Architecture
**Batch Pipeline**
- Apache Airflow orchestrates data ingestion workflows
- Raw data is stored in Amazon S3
- Apache Spark performs batch data processing
- Processed data is loaded into Snowflake Data Warehouse
- Analytics dashboards are built using Power BI

**Streaming Pipeline**
- Real-time data is ingested via Apache Kafka
- PySpark Structured Streaming processes the stream
- Aggregated data is stored in TimescaleDB
- Real-time monitoring is visualized using Grafana

## Technologies Used
- Apache Kafka
- Apache Spark (Batch & Structured Streaming)
- Apache Airflow
- Amazon S3
- Snowflake
- TimescaleDB
- Power BI
- Grafana
- Docker
- Python

## Key Outcomes
- Unified batch and streaming data processing
- Scalable and production-like architecture
- Real-time and historical traffic analytics

