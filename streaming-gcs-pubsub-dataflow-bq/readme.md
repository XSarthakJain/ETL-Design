# Streaming ETL Pipeline: GCS → Pub/Sub → Dataflow → BigQuery

## 📌 Overview

This project demonstrates a real-time streaming ETL pipeline using Google Cloud services.
Files uploaded to Cloud Storage trigger Pub/Sub notifications, which are processed by a Dataflow pipeline and loaded into BigQuery.

---

## 🏗️ Architecture

GCS → Pub/Sub → Dataflow → BigQuery

---

## ⚙️ Tech Stack

* Apache Beam (Python)
* Google Cloud Storage (GCS)
* Pub/Sub
* Dataflow
* BigQuery

---

## 🔄 Workflow

1. File uploaded to GCS
2. Notification sent to Pub/Sub
3. Dataflow reads message
4. Extracts file path
5. Reads file content
6. Parses transaction data
7. Loads into BigQuery

---

## 📊 Schema

* transaction_id (STRING)
* user_id (STRING)
* amount (FLOAT)
* currency (STRING)
* transaction_type (STRING)
* transaction_time (TIMESTAMP)
* merchant (STRING)
* location (STRING)
* status (STRING)

---

## 🚀 How to Run

1. Create Pub/Sub topic
2. Configure GCS notification
3. Deploy Dataflow pipeline
4. Upload CSV files to GCS

---

## ✅ Features

* Real-time ingestion
* Timestamp parsing
* Error handling (basic)
* Logging for debugging

---

## 🔥 Future Improvements

* Deduplication logic
* Dead Letter Queue (DLQ)
* Schema evolution handling
* Window-based aggregations

---

## 📂 Sample Data

Available in `sample_data/`

---

## 📸 Architecture Diagram

(Add screenshot here)
