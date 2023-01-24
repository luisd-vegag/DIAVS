# DIAVS (Data Integration and Validation System) is a system for performing data integration and validation on incoming data files.

## Overview 
DIAVS was developed to automate the process of data integration and validation, ensuring that data is properly formatted, and meets specific requirements before being consumed by downstream systems. The system is built on AWS services, including S3, Lambda, DynamoDB, CloudWatch, and SNS.

## System requirements 
- Data arrives in an S3 bucket in txt, csv, or xls format.
- Automatic trigger executes validations and transforms the data based on specific requirements.
- Data must be in Apache Parquet format and saved into an S3 bucket for consumption by downstream systems.

## Validations 
DIAVS performs the following validations on incoming data files:
- File name regex validation - File extension
- File encoding 
- File number of columns
- File common names

## Transformations 
DIAVS performs the following transformations on incoming data files:
- Add date column called "parameter_date" to the file with the date that comes in the file name. 
- Add date column called "source_date" to the file with the date that comes in the file name.
- Add date column called "file_date" to the file with the date from where the file is ingested.

## AWS services 
DIAVS uses the following AWS services:
- AWS Lambda to perform data validation and normalization, and use DynamoDB to store the validation and transformation rules. 
- CloudWatch is used to monitor the pipeline and logs.
- SNS is used to send notifications on pipeline status. 
- S3 is used to store the incoming data, intermediate data, and the final transformed data.
- DynamoDB is used to store the validation rules.

## Bucket structure 
DIAVS uses the following S3 bucket structure:
- "error-zone" is used to store files that fail validation or transformation. 
- "input-raw-zone" is used to store the original, unprocessed data files as they are received.
- "raw-zone" is used to store the data files after validation and normalization. 
- "landing-zone" is used to store the data files after transformation.
- "staging-zone" is used to store the final, transformed data files in Apache Parquet format.

## Data flow The data flow for DIAVS is as follows:
- Data files are ingested into the "input-raw-zone" S3 bucket. 
- The pipeline performs validations and transformations on the incoming file.
- If a file fails validation, it is moved to the "error-zone" S3 bucket. 
- If a file passes validation and normalization, it is moved to the "raw-zone" S3 bucket.
- If a file passes transformation, it is moved to the "landing-zone". 
- The file is transformed from CSV to Parquet and moved to the "staging-zone" S3 bucket.

## Special Note 
The DIAVS system was developed by using the help of OpenAI's ChatGPT model.