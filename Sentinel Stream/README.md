# 🚀 Real-Time Stock Price Aggregation Pipeline
### Apache Beam + Google Cloud Dataflow

![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python)
![Apache Beam](https://img.shields.io/badge/Apache%20Beam-2.50%2B-orange)
![Google Cloud](https://img.shields.io/badge/Google%20Cloud-Dataflow-4285F4?logo=google-cloud)
![PubSub](https://img.shields.io/badge/GCP-PubSub-yellow?logo=google-cloud)
![License](https://img.shields.io/badge/License-MIT-green)

---

## 📌 Overview

A **production-grade streaming data pipeline** built with **Apache Beam** and deployed on **Google Cloud Dataflow**. The pipeline ingests real-time stock ticker events from **Google Cloud PubSub**, reads corresponding CSV files from **Google Cloud Storage (GCS)**, deduplicates records using stateful processing, and aggregates running price totals per stock ticker — all in real time.

---

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  GCS Bucket │────▶│  Cloud PubSub│────▶│  Dataflow   │
│  (CSV Files)│     │ (Notification)│     │  Pipeline   │
└─────────────┘     └──────────────┘     └──────┬──────┘
                                                 │
                    ┌────────────────────────────▼──────────────────────────┐
                    │                  PIPELINE STAGES                       │
                    │                                                        │
                    │  PubSub Read → Extract GCS Path → Read CSV Lines      │
                    │       → Parse Rows → Fingerprint → Deduplicate        │
                    │       → GlobalWindow → CombinePerKey → Aggregated     │
                    │                      Output                            │
                    └────────────────────────────────────────────────────────┘
```

---

## ✨ Features

- **Real-time ingestion** from Google Cloud PubSub notifications
- **Dynamic GCS file reading** — automatically reads new CSV files uploaded to GCS
- **Stateful deduplication** using MD5 fingerprinting with 60-second expiry timer
- **Streaming-safe aggregation** using a custom `CombineFn` — no silent failures
- **GlobalWindow + AfterCount(1) trigger** — flushes results immediately without requiring watermark advancement
- **Robust error handling** with structured logging throughout every stage
- **Production-ready** — runs on Google Cloud Dataflow with DirectRunner support for local testing

---

## 🗂️ Project Structure

```
├── pipeline.py               # Main pipeline code
├── requirements.txt          # Python dependencies
├── README.md                 # Project documentation
└── sample_data/
    └── sample_stock.csv      # Sample input CSV for local testing
```

---

## 📋 Input Format

CSV files uploaded to GCS must follow this format:

```csv
timestamp,ticker,price,volume
2024-01-15 10:00:01,AAPL,182.50,1000
2024-01-15 10:00:02,GOOG,141.20,500
2024-01-15 10:00:03,AAPL,183.10,750
```

| Column | Type | Description |
|---|---|---|
| `timestamp` | `YYYY-MM-DD HH:MM:SS` | Event time of the tick |
| `ticker` | `string` | Stock ticker symbol (e.g. AAPL) |
| `price` | `float` | Stock price at that moment |
| `volume` | `float` | Trade volume |

---

## ⚙️ Pipeline Stages — Explained

### Stage 1 — Read PubSub
Reads GCS object notification messages from a PubSub subscription. Each message contains the bucket name and file name of a newly uploaded CSV.

### Stage 2 — Extract GCS Path
Parses the PubSub JSON notification and constructs the full `gs://bucket/file` path.

### Stage 3 — Read CSV from GCS
Uses `ReadAllFromText` to dynamically read the content of each CSV file from GCS.

### Stage 4 — Parse CSV Rows
Splits each line by comma, validates column count, skips the header row, and casts `price` and `volume` to `float`.

### Stage 5 — MD5 Fingerprinting
Creates a unique MD5 hash from `timestamp + ticker + price + volume` for each row. This fingerprint is the deduplication key.

### Stage 6 — Stateful Deduplication
Uses Apache Beam's **stateful DoFn** with `ReadModifyWriteState` to track seen fingerprints. Duplicate records are silently dropped. State expires after **60 seconds** via a real-time timer, allowing re-processing of the same record after expiry if needed.

### Stage 7 — Aggregation
Converts each clean record to a `(ticker, price)` key-value pair, applies **GlobalWindows with AfterCount(1) trigger**, and runs `CombinePerKey` with a custom `SumCombineFn` to accumulate running price totals per ticker.

> **Why GlobalWindows instead of FixedWindows?**
> `ReadAllFromText` is a bounded source — it reads a file and stops. It never advances Beam's watermark. `FixedWindows` relies on watermark advancement to flush the combiner, so with `ReadAllFromText` the aggregation would silently block forever. `GlobalWindows + AfterCount(1)` fires immediately on every element with no watermark dependency.

---

## 🛠️ Prerequisites

- Python 3.8+
- Google Cloud SDK installed and authenticated
- A GCP project with the following APIs enabled:
  - Cloud Dataflow API
  - Cloud PubSub API
  - Cloud Storage API

---

## 📦 Installation

```bash
# Clone the repository
git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate        # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

---

## 🔧 Configuration

Before running, update the following values inside `pipeline.py`:

```python
# 1. Path to your GCP service account key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path/to/your-key.json"

# 2. Your GCP project ID
options = PipelineOptions(
    streaming=True,
    project="your-gcp-project-id"
)

# 3. Your PubSub subscription
subscription="projects/your-project-id/subscriptions/your-subscription-name"
```

---

## ▶️ Running the Pipeline

### Local Testing (DirectRunner)
```bash
python pipeline.py \
  --runner=DirectRunner \
  --streaming
```

### Google Cloud Dataflow
```bash
python pipeline.py \
  --runner=DataflowRunner \
  --project=your-gcp-project-id \
  --region=us-central1 \
  --temp_location=gs://your-bucket/temp \
  --staging_location=gs://your-bucket/staging \
  --streaming
```

---

## 📤 Sample Output

Once running, you will see aggregated results in the logs:

```
2024-01-15 10:00:05 [INFO] GCS file detected: gs://my-bucket/stocks_batch_1.csv
2024-01-15 10:00:05 [INFO] Passed dedup: AAPL
2024-01-15 10:00:05 [INFO] Pre-aggregation: ('AAPL', 182.5)
2024-01-15 10:00:05 [INFO] RESULT — Ticker:   AAPL | Total: 182.5000
2024-01-15 10:00:06 [INFO] Passed dedup: GOOG
2024-01-15 10:00:06 [INFO] RESULT — Ticker:   AAPL | Total: 182.5000
2024-01-15 10:00:06 [INFO] RESULT — Ticker:   GOOG | Total: 141.2000
2024-01-15 10:00:07 [INFO] Duplicate dropped: AAPL
2024-01-15 10:00:08 [INFO] RESULT — Ticker:   AAPL | Total: 365.6000
```

---

## 📝 requirements.txt

```
apache-beam[gcp]==2.53.0
google-cloud-pubsub==2.18.4
google-cloud-storage==2.13.0
```

---

## 🐛 Common Issues & Fixes

| Issue | Cause | Fix |
|---|---|---|
| Aggregation prints nothing | `FixedWindows` blocked waiting for watermark | Use `GlobalWindows + AfterCount(1)` |
| `except(e)` SyntaxError | Invalid Python exception syntax | Use `except Exception as e` |
| Duplicate records passing through | `yield None` polluting PCollection | Remove `yield None` from dedup else branch |
| `sum()` not working in streaming | Builtin `sum` not streaming-safe | Use custom `SumCombineFn(beam.CombineFn)` |
| Codec error on PubSub decode | Wrong codec name `utf_8` | Use `utf-8` |

---

## 🔑 Key Design Decisions

**1. GlobalWindows over FixedWindows**
Since `ReadAllFromText` is a bounded source that never emits watermarks, `FixedWindows` would block the combiner indefinitely. `GlobalWindows` with a count-based trigger solves this cleanly.

**2. Custom SumCombineFn over builtin sum()**
Apache Beam's `CombinePerKey` distributes computation across workers using partial aggregations. The builtin `sum()` is not designed for this — a proper `CombineFn` with `create_accumulator`, `add_input`, `merge_accumulators`, and `extract_output` handles partial merges correctly across bundles.

**3. MD5 Fingerprint for Deduplication**
Rather than deduplicating on a single field, the fingerprint combines all four columns — ensuring that two records with the same ticker but different prices are treated as distinct events.

**4. 60-Second State Expiry**
The deduplication state clears after 60 seconds using a real-time timer. This prevents unbounded state growth in long-running streaming pipelines while still catching rapid duplicates.

---

## 👤 Author

**Sarthak Jain**
- GitHub: [@XSARTHAKJAIN](https://github.com/XSARTHAKJAIN)
- LinkedIn: [XSARTHAKJAIN](https://linkedin.com/in/XSARTHAKJAIN)

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.