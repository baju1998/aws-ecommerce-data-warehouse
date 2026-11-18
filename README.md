# AWS E-Commerce Data Warehouse

An end-to-end data engineering project that processes e-commerce transaction data using AWS cloud services, Apache Airflow, Python, PySpark, and SQL.

## Project Overview

This project demonstrates the design and implementation of an automated cloud-based data pipeline. It ingests e-commerce datasets into Amazon S3, transforms raw data using AWS Glue, and loads structured datasets into Amazon Redshift Serverless for analytical querying.

Apache Airflow orchestrates the pipeline, manages task dependencies, and performs data quality checks before refreshing production warehouse tables.

## Technology Stack

| Technology | Purpose |
|---|---|
| Amazon S3 | Raw and processed data storage |
| AWS Glue | Serverless ETL processing using PySpark |
| Amazon Redshift Serverless | Cloud data warehouse |
| Apache Airflow 2.8.1 | Pipeline orchestration and monitoring |
| Docker Compose | Local Airflow development environment |
| Python | Data ingestion, validation, and automation |
| SQL | Data modeling, transformations, and analytical queries |
| AWS IAM | Role-based access to AWS resources |

## Data Pipeline Architecture

The pipeline follows this workflow:

1. **Data ingestion:** Python scripts upload the source datasets into Amazon S3.
2. **ETL processing:** AWS Glue transforms the raw data into structured Parquet datasets.
3. **Staging:** Amazon Redshift loads transformed datasets from S3 into staging tables.
4. **Data validation:** Airflow checks record counts, primary-key uniqueness, and referential integrity.
5. **Production refresh:** Validated data is loaded into production warehouse tables.
6. **Analytics:** SQL queries support sales, revenue, and other e-commerce analyses.


flowchart TD
    A["Olist E-Commerce CSV Dataset"] --> B["Amazon S3: Raw Layer"]
    B --> C["AWS Glue: PySpark ETL"]
    C --> D["Amazon S3: Processed Parquet"]
    D --> E["Amazon Redshift: Staging Tables"]
    E --> F{"14 Data Quality Checks"}
    F -->|Pass| G["Redshift: Production Tables"]
    F -->|Fail| H["Stop Pipeline"]
    G --> I["SQL Analytics"]

    J["Apache Airflow Orchestration"] -.-> C
    J -.-> E
    J -.-> F
    J -.-> G


## Data Warehouse

The warehouse contains five primary tables:

| Table | Records |
|---|---:|
| dim_customer | 99,441 |
| dim_product | 32,951 |
| dim_seller | 3,095 |
| dim_date | 634 |
| fact_sales | 112,650 |
| **Total** | **248,771** |

The warehouse uses a dimensional model with a central sales fact table and four supporting dimensions.

## Data Quality

The pipeline performs 14 automated staging-data validation checks covering:

- Expected record counts across five tables
- Uniqueness of dimension primary keys
- Uniqueness of the composite sales transaction key
- Referential integrity between sales records and dimension tables

Validation failures stop the pipeline before the production refresh.

## Project Status

**Completed:** The full Apache Airflow DAG successfully executed from AWS Glue transformation through Redshift production refresh.

This repository documents a local-development implementation and is not configured as a production deployment.






## Local Development Setup

### Prerequisites

- Python 3.10 or later for local scripts
- Docker Desktop with Docker Compose
- AWS CLI v2
- An AWS account with access to Amazon S3, AWS Glue, and Amazon Redshift Serverless
- AWS IAM permissions appropriate for the pipeline

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/aws-ecommerce-data-warehouse.git
cd aws-ecommerce-data-warehouse
```

Replace `YOUR_USERNAME` with your GitHub username after publishing the repository.

### 2. Configure Python

Create and activate a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install the project's Python dependencies once they are documented in `requirements.txt`.

### 3. Configure AWS Authentication

Configure an AWS CLI profile with appropriate permissions.

For the local AWS IAM Identity Center (SSO) workflow:

```powershell
aws configure sso --profile ecommerce-dev
aws sso login --profile ecommerce-dev
aws sts get-caller-identity --profile ecommerce-dev
```

The profile name is configurable through `AWS_PROFILE`.

### 4. Configure Environment Variables

Copy `.env.example` to `.env`:

```powershell
Copy-Item .env.example .env
```

Update `.env` with your local AWS profile and development passwords.

Do not commit `.env` or AWS credential files to Git.

### 5. Start Apache Airflow

```powershell
docker compose up airflow-init
docker compose up -d
```

Open the Airflow interface at:

http://localhost:8080

### 6. Configure AWS Resources

Before running the pipeline, provision the required AWS resources:

- S3 bucket with `raw/` and `processed/` prefixes
- AWS Glue ETL job
- Redshift Serverless namespace, workgroup, database, and warehouse tables
- IAM roles and policies for accessing the relevant S3 objects

The example configurations contain project-specific resource names that must be adapted to your AWS environment.

### 7. Execute the Pipeline

In the Airflow interface:

1. Locate the `ecommerce_pipeline` DAG.
2. Trigger the DAG manually.
3. Monitor task execution in Grid view.
4. Confirm the staging validation task succeeds.
5. Verify that production tables contain the expected records.

### Development Notes

This project is designed as a local-development demonstration. It uses interactive AWS SSO authentication and development-oriented Docker Compose settings.

Production deployment would require stronger credential management, dedicated IAM roles, secure secret storage, and additional operational monitoring.