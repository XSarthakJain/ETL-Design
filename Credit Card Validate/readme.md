# 💳 Credit Card Transaction Streaming Pipeline

![GCP](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![Apache Beam](https://img.shields.io/badge/Apache_Beam-FF6B35?style=for-the-badge&logo=apache&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge&logo=googlebigquery&logoColor=white)
![Dataflow](https://img.shields.io/badge/Dataflow-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

A **real-time streaming data pipeline** built on **Google Cloud Platform** that ingests credit card transaction data, validates card numbers using the **Luhn Algorithm**, enforces data quality checks, and routes records to appropriate BigQuery tables — with **column-level security** to protect sensitive card data.

---

## 📐 Architecture Overview

```
Cloud Storage (CSV Drop)
        │
        ▼
   Cloud Pub/Sub
  (File Arrival Notification)
        │
        ▼
  Google Dataflow
  (Apache Beam Pipeline)
        │
        ├──── Side Inputs ──────────────────────┐
        │     • issuerName.csv                  │
        │     • cardtype.csv                    │
        │                                       │
        ├── [Parse & Validate] ◄────────────────┘
        │        │
        │        ├── Luhn Algorithm (Card Number Validation)
        │        ├── IssuerName Validation
        │        └── CardType Validation
        │
        ├──── ✅ Valid Records ──► BigQuery: credit_card_transactions
        │
        └──── ❌ Bad Records ───► BigQuery: bad_records (Dead Letter Queue)
```

---

## 🔁 Pipeline Flow

### 1. Data Ingestion
- Credit card transaction CSV files are uploaded to **Google Cloud Storage**.
- A **Pub/Sub notification** is triggered automatically when a new file lands in the bucket.
- The `getBucket` DoFn decodes the Pub/Sub message, extracts the `bucket` and `name` fields, and constructs the full GCS file path (`gs://bucket/filename`).

### 2. CSV Parsing — `parseData`
- Reads each line of the CSV and splits it into fields.
- Validates that essential fields (`Card_Number`, `CVV`, `ExpiryDate`) are present.
- Routes records using **Tagged Outputs**: `process` tag for valid records, `fail` tag for malformed ones.

**Expected Row Format:**
```
transaction_id, event_timestamp, Card_Number, CVV, ExpiryDate, Issued_Date,
Credit_Card_Name, First_Name, Last_Name, IssuerName, CardType
```

**Sample Record:**
```
b3788d8f, 2026-04-29 18:18:17, 9589024548849280, 633, 08/27, 10/23, SBI Elite, Isha, Mehta, SBI, VISA
```

### 3. IssuerName Validation — `isIssuerName` (Side Input)
- Reads valid issuer names from `issuerName.csv` as a **side input dictionary**.
- Checks whether the transaction's `IssuerName` exists in the allowed list.
- Invalid issuers are tagged and routed to the dead letter queue.

### 4. CardType Validation — `isCardType` (Side Input)
- Reads valid card types from `cardtype.csv` as a **side input dictionary**.
- Checks whether the transaction's `CardType` (e.g., VISA, Mastercard) is recognized.
- Invalid card types are tagged and routed to the dead letter queue.

### 5. Luhn Algorithm — `luhn`
- Implements the **Luhn checksum algorithm** to mathematically validate card numbers.
- Rejects alphanumeric card numbers immediately.
- Valid card numbers (checksum divisible by 10) are passed along; invalid ones go to the dead letter queue.

**How Luhn Works:**
```
1. Reverse the card number digits
2. Double every second digit (from index 1)
3. If the doubled value > 9, subtract 9
4. Sum all digits
5. If total % 10 == 0 → Valid ✅ else Invalid ❌
```

### 6. BigQuery Output
- **Valid records** → `myDataSet.credit_card_transactions` with `is_luhn_valid: TRUE`
- **All bad records** (from any stage) → `myDataSet.bad_records` with an `error` description

---

## 🗂️ BigQuery Schema

### ✅ `credit_card_transactions` (Valid Records)

| Column | Type | Description |
|---|---|---|
| `transaction_id` | STRING | Unique transaction identifier |
| `event_timestamp` | TIMESTAMP | Transaction datetime |
| `Card_Number` | STRING | Credit card number (policy-protected) |
| `CVV` | INTEGER | Card CVV |
| `ExpiryDate` | STRING | Card expiry (MM/YY) |
| `Issued_Date` | STRING | Card issue date |
| `Credit_Card_Name` | STRING | Name on card |
| `First_Name` | STRING | Cardholder first name |
| `Last_Name` | STRING | Cardholder last name |
| `IssuerName` | STRING | Issuing bank/institution |
| `CardType` | STRING | Card network (VISA, Mastercard, etc.) |
| `is_luhn_valid` | BOOLEAN | Luhn validation result (always TRUE here) |

### ❌ `bad_records` (Dead Letter Queue)

| Column | Type | Description |
|---|---|---|
| `transaction_id` | STRING | Transaction identifier |
| `event_timestamp` | TIMESTAMP | Transaction datetime |
| `Card_Number` | STRING | Card number (as received) |
| `CVV` | STRING | CVV (as received) |
| `ExpiryDate` | STRING | Expiry date |
| `Issued_Date` | STRING | Issue date |
| `Credit_Card_Name` | STRING | Name on card |
| `First_Name` | STRING | First name |
| `Last_Name` | STRING | Last name |
| `IssuerName` | STRING | Issuer name |
| `CardType` | STRING | Card type |
| `card_valid` | BOOLEAN | Always FALSE for bad records |
| `error` | STRING | Error description/reason for rejection |

---

## 🔒 Security: Column-Level Policy on `Card_Number`

A **BigQuery Column-Level Access Policy** has been applied to the `Card_Number` field in the `credit_card_transactions` table. This ensures:

- Only authorized users/service accounts with the designated IAM role can view the raw card number.
- Unauthorized users see `NULL` instead of the actual card number.
- Compliant with **PCI-DSS** data protection standards.

**To apply the policy:**
```sql
-- Grant column-level access to specific users
ALTER TABLE `<PROJECT_ID>.myDataSet.credit_card_transactions`
ADD COLUMN POLICY FILTER USING (SESSION_USER() IN ('authorised-user@domain.com'));
```

Or via BigQuery Policy Tags — create a taxonomy in Data Catalog, tag the `Card_Number` column, and control access via IAM.

---

## 📁 Side Input Files

### `issuerName.csv`
```
IssuerName,IsValid
SBI,true
HDFC,true
ICICI,true
...
```

### `cardtype.csv`
```
CardType,IsValid
VISA,true
Mastercard,true
RuPay,true
...
```

---

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- Google Cloud SDK installed and authenticated
- A GCP project with the following APIs enabled:
  - Cloud Dataflow API
  - Cloud Pub/Sub API
  - BigQuery API
  - Cloud Storage API

### Installation

```bash
# Clone the repository
git clone https://github.com/your-username/credit-card-streaming-pipeline.git
cd credit-card-streaming-pipeline

# Install dependencies
pip install apache-beam[gcp]
```

### Set Up GCP Credentials

```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
```

### Configure Pub/Sub & Cloud Storage

1. Create a GCS bucket for incoming CSV files.
2. Create a Pub/Sub topic and subscription for file arrival notifications.
3. Enable GCS notifications to publish to your Pub/Sub topic:
```bash
gsutil notification create \
  -t projects/YOUR_PROJECT/topics/YOUR_TOPIC \
  -f json \
  gs://YOUR_BUCKET
```

### Run the Pipeline

```bash
python pipeline.py \
  --project=<PROJECT_ID> \
  --region=us-central1 \
  --runner=DataflowRunner \
  --temp_location=gs://YOUR_BUCKET/temp \
  --streaming
```

---

## 📦 Project Structure

```
credit-card-streaming-pipeline/
│
├── pipeline.py              # Main Apache Beam pipeline
├── issuerName.csv           # Side input: valid issuer names
├── cardtype.csv             # Side input: valid card types
├── requirements.txt         # Python dependencies
└── README.md
```

---

## 🔖 Key Concepts Used

| Concept | Usage |
|---|---|
| **Streaming Pipeline** | `PipelineOptions(streaming=True)` for unbounded Pub/Sub source |
| **Tagged Outputs** | `pvalue.TaggedOutput` with `process` / `fail` tags to split records |
| **Side Inputs** | `beam.pvalue.AsDict()` for IssuerName and CardType lookups |
| **Dead Letter Queue** | `fail` tagged outputs from all stages flattened into `bad_records` |
| **Luhn Algorithm** | Mathematical card number checksum validation |
| **Beam Flatten** | Merging multiple bad-record PCollections into one |
| **WriteToBigQuery** | Streaming inserts with `WRITE_APPEND` + `CREATE_IF_NEEDED` |

---

## 🔮 Future Scope

- **Fraud Detection by Location**: Enrich transactions with IP/geolocation data and flag transactions originating from unusual or high-risk locations. Build a heat map of fraudulent card usage across cities/countries.
- **Real-time Alerting**: Integrate with Cloud Monitoring or send Pub/Sub alerts when fraud thresholds are breached.
- **ML-based Anomaly Detection**: Use Vertex AI to detect unusual spending patterns (velocity checks, unusual merchants, late-night transactions).
- **Card Velocity Checks**: Flag multiple transactions from the same card number within a short time window using Apache Beam's windowing and stateful processing.
- **Dashboard**: Build a Looker Studio (Data Studio) dashboard on top of BigQuery for real-time fraud monitoring.

---

## 👤 Author

**Your Name**
- GitHub: [@XSARTHAKJAIN](https://github.com/XSARTHAKJAIN)
- LinkedIn: [XSARTHAKJAIN](https://linkedin.com/in/XSARTHAKJAIN)

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.