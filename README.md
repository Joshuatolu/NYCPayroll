># NYC Hospital Data Pipeline
This project is an ETL (Extract, Transform, Load) pipeline designed to process payroll, employee, agency, and title data for NYC Hospital. The pipeline extracts data from S3 buckets, transforms it using AWS Glue, and loads the cleaned and validated data into an AWS RDS PostgreSQL database. Additionally, it generates aggregate tables for business analysis using a stored procedure.

>## Table of Contents
1. Overview
2. Technologies Used
3. Pipeline Workflow
4. Aggregation Process
5. Setup and Configuration
6. Code Structure
7. How It Works
8. Archiving Process
9. Contributing

>## Overview
The NYC Hospital Data Pipeline is built to handle large-scale data processing tasks. It extracts raw data from multiple S3 buckets, performs data cleaning and transformation using AWS Glue, and loads the processed data into a PostgreSQL database. The pipeline also archives source files after processing and generates aggregate tables for business insights.

>## Technologies Used
- AWS Glue: Serverless ETL service for data extraction, transformation, and loading.
- AWS S3: Cloud storage for raw and archived data.
- AWS RDS PostgreSQL: Relational database for storing processed data.
- PySpark: Data processing framework for handling large datasets.
- Boto3: AWS SDK for Python to interact with S3.
- PostgreSQL JDBC: Connector for writing data to the RDS database.
- PL/pgSQL: Language for creating stored procedures in PostgreSQL.

>## Pipeline Workflow

1. Extract:
    + Data is extracted from S3 buckets:
      + s3://xxxx/nyc_sourcedata/PayrollData/
      + s3://xxxx/nyc_sourcedata/EmployeeData/
      + s3://xxxx/nyc_sourcedata/TitleData/
      + s3://xxxx/nyc_sourcedata/AgencyData/

2. Transform:

    + Data is cleaned and transformed using PySpark:
      + Handled null values, typecasting, and column standardization.
      + Merged and renamed columns (e.g., agencycode to agencyid).
      + Filter invalid rows (e.g., rows with NULL or 0 values in RegularGrossPaid or RegularHours).

3. Load:

    + Valid data is loaded into PostgreSQL tables:
      - Employee
      - Payroll
      - Title
      - Agency
    + Invalid data is stored in a separate table (payroll_data_issues) for review.

4. Aggregate:
     + A stored procedure (stg.GenerateAggregateTables) is executed to generate aggregate tables for business analysis.
5. Archive:

    + Source files are moved to an archive folder in S3 after processing.

>## Aggregation Process
The stored procedure stg.GenerateAggregateTables creates the following aggregate tables in the dev schema:

1. Payroll Aggregates: Total salary, overtime, and other payments by fiscal year, agency, and title.
2. Salary Analysis: Average and total compensation by fiscal year, agency, and title.
3. Workforce Distribution: Employee count by fiscal year, agency, work location, and title.
4. Leave Patterns: Employee leave statistics by fiscal year, agency, and title.
5. Overtime Analysis: Overtime hours and costs by fiscal year, agency, and title.
6. Employee Hiring Aggregates: Hiring trends by year and agency.
7. Overtime Aggregates: Overtime usage and costs by fiscal year, agency, and title.
8. Agency Compensation Summary: Total compensation and employee count by fiscal year and agency.

## Setup and Configuration
### Prerequisites
  - AWS account with access to S3, Glue, and RDS.
  - Python 3.x and PySpark installed.
  - PostgreSQL database set up in AWS RDS.

>### Configuration
1. S3 Configuration:

    - Upload raw data files to the source S3 buckets.
    - Create archive folders for processed data.

2. RDS Configuration:

    - Set up a PostgreSQL database in AWS RDS.
    - Update the config.json file with RDS connection details:

```json
{
  "rds_user": "your_username",
  "rds_password": "your_password",
  "rds_host": "your_rds_endpoint",
  "rds_port": "5432",
  "rds_db_name": "your_database_name",
  "rds_schema": "your_schema_name"
}
```

3. Upload config.json to the S3 bucket (s3://xxxx/env_file/config.json).

    - AWS Glue Configuration:
      - Create a Glue job with the provided script.
      - Set the following parameters:
```
%idle_timeout 2880
%glue_version 5.0
%worker_type G.1X
%number_of_workers 5
```

4. Stored Procedure:

    + Execute the stg.GenerateAggregateTables procedure in your PostgreSQL database to create the aggregate tables.

>## Code Structure
The pipeline is implemented in a single Python script. Key components include:

  + Data Extraction: Reads data from S3 buckets into DynamicFrames.
  + Data Transformation: Cleans and transforms data using PySpark.
  + Data Loading: Writes valid data to PostgreSQL tables and invalid data to a separate table.
  + Aggregation: Executes a stored procedure to generate aggregate tables.
  + Archiving: Moves processed files to an archive folder in S3.

>## How It Works
1. Initialize GlueContext:

    - The script initializes a GlueContext and SparkSession for data processing.

2. Load Data:

    - Data is loaded from S3 into DynamicFrames and converted to PySpark DataFrames.

3. Transform Data:

    + Columns are standardized, and invalid rows are filtered out.

4. Write to RDS:

    + Valid data is written to PostgreSQL tables using JDBC.
  
5. Execute Stored Procedure:

    + The stg.GenerateAggregateTables procedure is executed to generate aggregate tables.

6. Archive Files:

    + Source files are moved to an archive folder in S3.

>### Archiving Process
After processing, source files are moved from the source folder to an archive folder in S3. This ensures that:

  + Processed files are not reprocessed.
  + Raw data is preserved for auditing and debugging.

## Contributing
Contributions are welcome! If you'd like to contribute:

  + Fork the repository.

      + Create a new branch (git checkout -b feature/YourFeature).

      + Commit your changes (git commit -m 'Add some feature').

      + Push to the branch (git push origin feature/YourFeature).

      + Open a pull request.

This README provides a comprehensive overview of my work, making it easy for anyone to understand and use. Let me know if you need any information or you want us to collaborate! You can reach me on toluseonibiyo@gmail.com. Thank You!!!
