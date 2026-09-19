# UPI Payment Data Engineering Pipeline

An end-to-end **UPI Payment Data Engineering pipeline** built using **PySpark and Delta Lake**, demonstrating Bronze, Silver, Quarantine, and Gold data layers with comprehensive data quality validation and transformation.

## 📌 Project Overview

A payment company receives daily UPI transaction data from multiple source systems. The incoming data may contain inconsistent formats, invalid values, duplicate transactions, and invalid merchant references.

This project demonstrates how raw payment transaction data can be transformed into structured, analytics-ready datasets using a modern data engineering pipeline.

### Pipeline Architecture

text
Raw Source
    │
    ▼
Bronze Layer
    │
    ▼
Silver Layer
    │
    ├── Data Cleaning
    ├── Standardization
    ├── Data Quality Validation
    ├── Duplicate Detection
    └── Referential Integrity
    │
    ├───────────────► Quarantine
    │
    ▼
Gold Layer
    │
    ├── Transaction-Level Analytics
    ├── Merchant Performance
    └── Daily Payment Summary
    │
    ▼
Analytics / Power BI

🛠️ Technologies Used
Python
PySpark
Apache Spark
Delta Lake
Spark SQL
Window Functions
Regular Expressions
Data Quality Validation
Medallion Architecture
GitHub
📂 Data Source

The source dataset contains UPI payment transaction records with the following fields:

Transaction_ID
Payment_Method
UPI_ID
Phone
Amount
Transaction_Status
Transaction_Timestamp
Merchant_ID
Merchant_Name
City

The dataset intentionally contains messy and invalid values to simulate real-world payment data.

Examples include:

Inconsistent Transaction ID formats
Invalid UPI IDs
Multiple phone number formats
Currency symbols and formatted amounts
K/L amount representations
Invalid transaction statuses
Multiple timestamp formats
Invalid Merchant IDs
Duplicate transactions
Orphan Merchant IDs
Inconsistent merchant and city formatting
🥉 Bronze Layer

The Bronze layer stores the source data in its original form.

Responsibilities
Ingest raw source data
Preserve original values
Avoid business-level transformations
Write raw data as Delta
Read the Bronze Delta data before beginning Silver transformations
Source Dataset
      ↓
Bronze DataFrame
      ↓
Bronze Delta Table
🥈 Silver Layer

The Silver layer performs data cleaning, standardization, validation, and business-rule processing.

Original values are preserved using raw_ columns while cleaned values are stored in the corresponding business columns.

For example:

raw_Phone
Phone
valid_Phone
🔍 Data Quality Rules
DQ001 — Transaction ID

Expected format:

TXN-YYYY-NNNNNN

Example:

TXN-2026-100001

Processing includes:

Trim whitespace
Convert to uppercase
Replace _ with -
Normalize unambiguous missing separators
Validate using regular expression

Invalid Transaction IDs are set to NULL.

DQ002 — Payment Method

Only the following value is valid:

UPI

Processing includes:

Trim
Convert to uppercase

Invalid values are set to NULL.

DQ003 — UPI ID

UPI IDs are standardized by:

Trimming whitespace
Converting to lowercase
Removing accidental spaces
Validating username and handle structure

Example:

rahul.kumar@okaxis
priya-sharma@okhdfc

Invalid UPI IDs are set to NULL.

DQ004 — Phone

Multiple source formats are normalized into a 10-digit Indian mobile number.

Examples:

9876543210
98765-43210
98765 43210
+91-9876543210
09876543210
91 9876543210

Final validation:

^[6-9]\d{9}$

Invalid phone numbers are set to NULL.

DQ005 — Amount

The pipeline handles different amount representations such as:

1250.50
₹2,499
1,250.75
2.5K
10K
₹5L
1.25L

Conversions include:

2.5K  → 2500
10K   → 10000
5L    → 500000
1.25L → 125000

Amounts must be numeric and greater than zero.

Invalid amounts are set to NULL.

DQ006 — Transaction Status

Valid statuses:

SUCCESS
FAILED
PENDING

Processing includes:

Trim
Convert to uppercase

Invalid values such as COMPLETED, PEND, and UNKNOWN are rejected.

DQ007 — Transaction Timestamp

The pipeline supports multiple source timestamp formats, including:

2026/02/01 09:15:23
2026-02-01 11:05:42
01-02-2026 10:22:11
07/02/2026 12:30:10

Values are converted to Spark TimestampType.

Invalid timestamps are set to NULL.

DQ008 — Merchant ID

Merchant IDs must follow:

^M\d{3}$

Example:

M001
M025
M039

Merchant ID format validation is kept separate from merchant master referential integrity.

DQ009 — Duplicate Transactions

Duplicate transactions are identified using:

Transaction_ID

Window functions and row_number() are used to identify duplicate occurrences.

One occurrence is retained while subsequent duplicate occurrences are identified separately for quarantine and audit purposes.

DQ010 — Merchant Referential Integrity

A separate Merchant Master dataset is used to validate Merchant IDs.

The transaction Merchant ID must exist in the Merchant Master.

The pipeline uses a:

LEFT ANTI JOIN

to identify orphan Merchant IDs.

Example:

M999

is identified as an orphan because it does not exist in the Merchant Master.

🚨 Quarantine Layer

Invalid data is isolated into a separate quarantine dataset using an Option-B field-level quarantine design.

Quarantine Schema
Transaction_ID
Column_Name
Invalid_Value
Rule_ID
Failure_Reason

Example:

TXN-2026-100023
Transaction_Status
COMPLETED
DQ006
Invalid transaction status

A single transaction can generate multiple quarantine records when multiple fields fail different data quality rules.

This approach preserves auditability and makes it possible to understand exactly why a record or field failed validation.

🥇 Gold Layer

The Gold layer contains analytics-ready datasets derived from the Silver layer.

1. Gold Transaction Dataset

gold_upi_transactions_df

Contains cleaned transaction-level information along with analytical date attributes.

Key fields include:

Transaction_ID
Transaction_Date
Transaction_Timestamp
Payment_Method
UPI_ID
Phone
Amount
Transaction_Status
Merchant_ID
Merchant_Name
City
Transaction_Year
Transaction_Month
Transaction_Day
2. Merchant Performance Summary

gold_merchant_summary_df

Provides merchant-level metrics such as:

Total transactions
Successful transactions
Failed transactions
Pending transactions
Total transaction amount
Successful transaction amount
Average transaction amount
Success rate
3. Daily Payment Summary

gold_daily_payment_summary_df

Provides daily payment metrics including:

Total transactions
Successful transactions
Failed transactions
Pending transactions
Total transaction amount
Successful transaction amount
Average transaction amount

These Gold datasets can be consumed by analytics tools such as Power BI.

📁 Project Structure
UPI-Payment-Data-Engineering-Pipeline/
│
├── README.md
│
├── requirements.txt
│
└── notebooks/
    │
    └── UPI_Payment_Data_Engineering_V2.ipynb
▶️ How to Run
1. Clone the repository
git clone https://github.com/shyamsingha-DE/UPI-Payment-Data-Engineering-Pipeline.git
2. Install dependencies
pip install -r requirements.txt
3. Open the notebook

Open:

notebooks/UPI_Payment_Data_Engineering_V2.ipynb
4. Run the notebook

Execute the notebook sequentially from:

Source Data
    ↓
Bronze
    ↓
Silver
    ↓
Data Quality Validation
    ↓
Quarantine
    ↓
Gold
🎯 Key Data Engineering Concepts Demonstrated

This project demonstrates practical experience with:

PySpark DataFrames
DataFrame transformations
withColumn
select
filter
when / otherwise
regexp_replace
regexp_extract
Regular expressions
Type casting
try_cast
try_to_timestamp
coalesce
Aggregations
Window functions
row_number
groupBy
LEFT ANTI JOIN
Delta Lake
Bronze/Silver/Gold architecture
Data quality rules
Data quarantine
Referential integrity
Duplicate detection
Analytics-ready Gold datasets
💼 Business Value

The pipeline demonstrates how a payment organization can transform inconsistent transactional data into structured datasets suitable for:

Payment analytics
Merchant performance reporting
Transaction monitoring
Data quality monitoring
Business intelligence
Power BI dashboards
Downstream analytical workloads
👨‍💻 Author

Shyam Singha

QA Lead / Testing Professional transitioning into Data Engineering & Analytics.

Areas of Focus
Data Engineering
PySpark
Databricks
SQL
Power BI
Data Quality
Payments
UPI / NPCI / Visa
📌 Project Status

Completed

This project was developed as a hands-on Data Engineering portfolio project to demonstrate an end-to-end payment data pipeline using PySpark and Delta Lake.
