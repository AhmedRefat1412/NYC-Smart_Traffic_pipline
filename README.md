#  NYC Smart Traffic Pipeline

A production-scale Data Engineering platform designed to process both historical and real-time NYC traffic data for urban mobility analytics.

---

##  Project Overview

This project builds a scalable data platform that ingests, processes, and analyzes traffic, weather, and incident data across New York City.
The architecture combines **Batch Processing** and **Real-Time Streaming** to deliver both historical insights and live monitoring capabilities.

---

## 🏗️ Architecture

![Architecture](https://github.com/AhmedRefat1412/NYC-Smart_Traffic_pipline/blob/main/docs/nyc%20_traffic.drawio.png)

---

## 📊 Dashboard

![Dashboard](https://github.com/AhmedRefat1412/NYC-Smart_Traffic_pipline/blob/main/docs/nyc_dashbord.pdf)

Interactive dashboards built using:
- Power BI → Business insights  
- Grafana → Real-time monitoring  
---

##  Data Lake Design (S3)

The data lake follows a **multi-layered architecture**:

- **Raw Layer (S3 Bucket 1)**  
  Stores ingested data in its original format

- **Processed Layer (S3 Bucket 2)**  
  Cleaned and transformed data using PySpark

- **Curated Layer (S3 Bucket 3)**  
  Structured data (Fact & Dimension tables) ready for analytics

---

##  Data Pipeline

### 🟡 Batch Pipeline
- Orchestrated using Apache Airflow  
- Data ingestion → S3 (Raw)  
- Processing using Apache Spark (PySpark)  
- Transformation into analytics-ready datasets  

### 🔴 Streaming Pipeline
- Real-time ingestion using Apache Kafka  
- Processing using PySpark Structured Streaming  
- Near real-time analytics delivery  

---

##  Data Warehouse Design (Galaxy Schema)

![Galaxy Schema](https://github.com/AhmedRefat1412/NYC-Smart_Traffic_pipline/blob/main/docs/nyc_dw_schema.jpeg)

Designed a **Galaxy Schema (Fact Constellation)** where multiple fact tables share conformed dimension tables.

This approach enables:
- Cross-domain analytics (traffic, weather, incidents)  
- High-performance querying in Snowflake  
- Scalable and flexible data modeling  

The schema supports complex analytical queries across multiple business processes.


---

##  Data Scale

- Processed **4M+ records**  
- Designed for scalability and high performance  



---

##  Tech Stack

- AWS S3 (Data Lake)  
- Apache Spark (PySpark)  
- Apache Airflow  
- Apache Kafka  
- Snowflake  
- TimescaleDB  
- Power BI  
- Grafana  

---

## 🎯 Key Features

- End-to-End Data Pipeline (Batch + Streaming)  
- Multi-layered Data Lake Architecture  
- Real-time & Historical Analytics  
- Scalable Distributed Processing  
- Galaxy Schema (Fact Constellation Modeling)  
- Production-style Orchestration  

---
m  

---

## 🤝 Feedback

I would really appreciate your feedback and suggestions to improve this project.
