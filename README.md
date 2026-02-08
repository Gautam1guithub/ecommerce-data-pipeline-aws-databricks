# E-commerce Data Pipeline & Analytics using AWS and Databricks

## 📌 Project Overview
This project demonstrates an **end-to-end data engineering pipeline** built using AWS and Databricks to simulate a real-world **e-commerce analytics use case**.  
The pipeline follows a **layered architecture (Bronze → Silver)** to ensure data quality, scalability, and efficient analytics.

---

## 🏗️ Architecture
<img width="1536" height="1024" alt="ChatGPT Image Jan 29, 2026, 09_23_43 AM" src="https://github.com/user-attachments/assets/137b4872-4acc-4943-9224-4c74831818b6" />

**High-level Flow:**
- Synthetic e-commerce data is generated using **AWS Lambda**
- Raw data is stored in **Amazon S3 (Bronze Layer)**
- Data is cleaned and transformed using **AWS Glue (PySpark)**
- Transformed data is stored in **S3 (Silver Layer) in Parquet format**
- Data is queried and analyzed using **Databricks with Unity Catalog**

---

## 🔄 Data Pipeline Flow

### 1️⃣ Data Generation (Bronze Layer)
- AWS Lambda generates synthetic e-commerce order data
- Data includes:
  - Orders, customers, products
  - Pricing, discounts, payment methods
  - Order status and city information
- Output format: **CSV**
- Storage: **Amazon S3 (Raw/Bronze Bucket)**

---

### 2️⃣ ETL Processing (Silver Layer)
- AWS Glue job written in **PySpark**
- Explicit schema applied to prevent schema drift
- Data cleaning:
  - Null value handling
  - Data type casting
- Output format converted to **Parquet**
- Storage: **Amazon S3 (Silver Layer Bucket)**

---

### 3️⃣ Analytics & Querying
- Silver layer data accessed using **Databricks External Location**
- Managed via **Unity Catalog**
- Data validated and analyzed using **PySpark queries**
- Ready for downstream analytics and reporting

---

## 🛠️ Tech Stack
- **AWS Lambda**
- **Amazon S3**
- **AWS Glue**
- **PySpark**
- **Databricks**
- **Unity Catalog**
- **Parquet**

---

## 📁 Project Structure


