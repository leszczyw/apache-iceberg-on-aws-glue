# Apache Iceberg with AWS Glue Example

This project demonstrates how to create and query Apache Iceberg tables using PySpark with AWS Glue Data Catalog integration.

## Prerequisites

- Python 3.8+
- Apache Spark with Iceberg support
- AWS account with Glue and S3 access
- AWS credentials configured on your system or environment

## Setup

1. Install required Python packages:
`pip install -r requirements.txt`

2. Configure Spark to use the Glue Data Catalog for Iceberg.

## Running the Example

Run the example script:

```
spark-submit --packages org.apache.iceberg:iceberg-spark-runtime:1.2.0
iceberg_glue_example.py
```

Make sure the Glue catalog and AWS credentials are properly set.

## Notes

This is a basic example. Extend it according to your use case and AWS environment setup.
