# data-integration-and-validation-system
# ChatGPT-3 setting up promt:
I want you to act as a data engineer with experience in AWS Services. 

Your goal is to help me develop a Data integration and validation system which we are going to call "DIAVS". 

Here is some information about the system:

System requirements:
/Data arrives in an S3 bucket in txt, csv, or xls format.
/Automatic trigger executes validations and transforms the data based on specific requirements.
/Data must be in Apache Parquet format and saved into an S3 bucket for consumption by downstream systems.

Validations:
/File name regex validation
/File extension
/File encoding
/File number of columns
/File columns names

Transformations:
/Add a date column called "parameter_date" to the file with the date that comes in the file name.
/Add a date column called "source_date" to the file with the date that comes in the file name.
/Add a date column called "file_date" to the file with the from where the file is ingested.

AWS services:
/Use AWS Lambda to perform data validation and normalization, and use DynamoDB to store the validation and transformation rules.
/CloudWatch is used to monitor the pipeline and logs.
/SNS is used to send notifications on pipeline status.
/S3 is used to store the incoming data, intermediate data, and final transformed data.
/DynamoDB is used to store the validation rules.

Bucket structure:
/"error-zone" is used to store files that fail validation or transformation.
/"input-raw-zone" is used to store the original, unprocessed data files as they are received.
/"raw-zone" is used to store the data files after validation and normalization.
/"landing-zone" is used to store the data files after transformation.
/"staging-zone" is used to store the final, transformed data files in Apache Parquet format.

Data flow:
/Data files are ingested into the "input-raw-zone" S3 bucket.
/The pipeline performs validations and transformations for the incoming file.
/If a file fails validation, it is moved to the "error-zone" S3 bucket.
/If a file passes validation and normalization, it is moved to the "raw-zone" S3 bucket.
/If a file passes transformation, it is moved to the "landing-zone".
/File is transformed from CSV to Parquet and moved to "staging-zone" S3 bucket. 

Keep track of your available tokens, if you have 20 tokens left STOP writing and say "Running out of tokens", then wait for me to say "continue". When I say "continue" you must start your response with the last 20 tokens from your last response. After every response add the number of tokens used over how many tokens you had available for the response, formatted like (tokens used: x).

Before starting read this prompt and tell me if you have any doubts. If you do not have any doubt say "I have no doubts, let us start." and wait for my next instruction.