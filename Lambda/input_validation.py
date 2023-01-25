import boto3
import json
import re
import os
import io
from datetime import datetime

# Packages from layers
import cchardet
import pandas as pd

REGION = os.getenv("REGION")
s3 = boto3.client("s3")
sns = boto3.client("sns")
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table("inventory_per_district")


def get_topic_arn(topic_name):
    """
    This function retrieves the ARN of a topic with the specified name.
    It retrieves all the topics available in the SNS service and looks 
    for the topic with the specified name.
    """
    response = sns.list_topics()
    topics = response["Topics"]
    next_token = response.get("NextToken", None)
    while next_token:
        response = sns.list_topics(NextToken=next_token)
        topics += response["Topics"]
        next_token = response.get("NextToken", None)
    for topic in topics:
        if topic["TopicArn"].split(":")[-1] == topic_name:
            return topic["TopicArn"]
    return None


def validate_file_extension(file_name, expected_extension):
    """
    This function validates the file extension of a file.
    It takes the file name and the expected extension as input and
    returns True if the file extension matches the expected extension.
    """
    file_extension = file_name.split(".")[-1]
    if file_extension == expected_extension:
        return True
    return False


def validate_file_encoding(file_content, expected_encoding):
    """
    This function validates the encoding of a file.
    It takes the file content and the expected encoding as input and
    returns True if the file encoding matches the expected encoding.
    """
    result = cchardet.detect(file_content)
    if result["encoding"].lower() == expected_encoding:
        return True
    return False


def normalize_headers(file_content, delimiter):
    """
    This function normalizes the headers of a file.
    It takes the file content and the delimiter as input and
    returns the file content with the headers normalized.
    """
    lines = file_content.split("\n")
    headers = lines[0].strip().split(delimiter)
    normalized_headers = [
        re.sub(r"[^a-zA-Z0-9_ ]", "", header)
        .replace(" ", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("-", "_")
        .lower()
        for header in headers
    ]
    lines[0] = delimiter.join(normalized_headers)
    return "\n".join(lines)


def validate_file_number_of_columns(file_content, expected_columns_count, delimiter):
    """
    This function validates the number of columns in a file.
    It takes the file content, the expected number of columns and the delimiter as input and
    returns True if the number of columns in the file matches the expected number of columns.
    """
    columns_count = len(file_content.split("\n")[0].split(delimiter))
    if columns_count == expected_columns_count:
        return True
    return False


def validate_file_columns_names(file_content, columns_details, delimiter):
    """
    This function validates the names of columns in a file.
    It takes the file content, the columns details and the delimiter as input and
    returns True if the names of columns in the file match the expected names of columns.
    """
    columns_names = file_content.split("\n")[0].strip().split(delimiter)
    expected_columns_names = [col['header'] for col in columns_details]
    if all(header in expected_columns_names for header in columns_names):
        return True
    return False


def file_content_to_df(file_content, columns_details, delimiter, encoding):
    """
    This function converts the content of a file to a pandas dataframe.
    It takes the file content, the columns details, the delimiter and the encoding as input and
    returns a pandas dataframe with the data from the file.
    """
    dtypes = {}
    for col in columns_details:
        column_name = col["header"]
        column_type = col["data_type"]
        if column_type == "date":
            dtypes[column_name] = "string"
        else:
            dtypes[column_name] = column_type
    df = pd.read_csv(io.BytesIO(bytes(file_content, encoding)),
                     dtype=dtypes, delimiter=delimiter)

    for col in columns_details:
        if col["data_type"] == "date" and "date_format" in col:
            df[col["header"]] = pd.to_datetime(
                df[col["header"]], format=col["date_format"])

    return df


def add_date_columns(df, date_details, file_name):
    """
    This function adds date columns to a pandas dataframe.
    It takes the dataframe, the date details and the file name as input and
    returns the dataframe with additional date columns.
    """
    if "parameter_date" in date_details:
        df["parameter_date"] = pd.to_datetime(
            df[date_details["parameter_date"]], format="%Y-%m-%d")
    if "source_date" in date_details:
        source_date = re.search(
            date_details["source_date"]["date_regex"], file_name).group()
        df["source_date"] = source_date
        df["source_date"] = pd.to_datetime(
            df["source_date"], format=date_details["source_date"]["date_format"])
        df["source_date"] = pd.to_datetime(
            df["source_date"], format="%Y-%m-%d")

    if "file_date" in date_details and date_details["file_date"] == True:
        now = datetime.now().strftime("%Y-%m-%d")
        df["file_date"] = pd.to_datetime(now, format="%Y-%m-%d")
    return df


def lambda_handler(event, context):
    """
    This function is the entry point for the Lambda function.
    It takes the event and the context as input and
    performs validation and processing on the file in S3.
    """
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    prefix = event["Records"][0]["s3"]["object"]["key"]

    document_key = "/".join(re.split("/", prefix)[:-1])
    file_name = prefix.rsplit("/", 1)[-1]

    response = table.get_item(Key={"document_key": document_key})
    district_documents = response.get("Item")

    valid_file = False
    valid_sns_topic = False
    for _, district_data in district_documents["files"].items():
        if re.match(district_data["file_name_regex"], file_name):
            district_key = district_data["district_key"]
            print(file_name)
            valid_file = True
    if valid_file:
        response = table.get_item(Key={"document_key": district_key})
        district_rules = response.get("Item")
        if "file_extension" in district_rules["validation_rules"]:
            extension_status = validate_file_extension(
                file_name, district_rules["validation_rules"].get(
                    "file_extension")
            )
            print(f"extension_status: {extension_status}")
        obj = s3.get_object(Bucket=bucket_name, Key=prefix)
        file_content = obj["Body"].read()
        encoding = district_rules["validation_rules"].get("encoding")
        if "encoding" in district_rules["validation_rules"] and extension_status:
            encoding_status = validate_file_encoding(file_content, encoding)
            print(f"encoding_status: {encoding_status}")
        if encoding_status:
            delimiter = district_rules["validation_rules"].get("delimiter")
            file_content = file_content.decode(encoding)
            file_content = normalize_headers(file_content, delimiter)
            if "columns_count" in district_rules["validation_rules"]:
                number_of_columns_status = validate_file_number_of_columns(
                    file_content,
                    district_rules["validation_rules"].get("columns_count"),
                    delimiter,
                )
                print(f"number_of_columns_status: {number_of_columns_status}")

            if "columns_details" in district_rules["validation_rules"]:
                columns_details = district_rules["validation_rules"].get(
                    "columns_details")
                columns_names_status = validate_file_columns_names(
                    file_content,
                    columns_details,
                    delimiter,
                )
                print(f"columns_names_status: {columns_names_status}")
                if "date_details" in district_rules["validation_rules"]:
                    date_details = district_rules["validation_rules"].get(
                        "date_details")
                df = file_content_to_df(
                    file_content, columns_details, delimiter, encoding)
                df = add_date_columns(df, date_details, file_name)

        sns_topic_name = "_".join(document_key.split("/")).lower()
        topic_arn = get_topic_arn(sns_topic_name)
        if topic_arn:
            valid_sns_topic = True

    # if valid_sns_topic:
    #    response = sns.publish(
    #        TopicArn=topic_arn, Message=f"New file '{file_name}' at '{document_key}'"
    #    )
