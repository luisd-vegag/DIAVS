import boto3
import json
import re
import os

# import chardet <-- Adding python layer in progress
import io
from datetime import datetime

REGION = os.getenv("REGION")
s3 = boto3.client("s3")
sns = boto3.client("sns")
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table("inventory_per_district")


def get_topic_arn(topic_name):
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
    file_extension = file_name.split(".")[-1]
    if file_extension == expected_extension:
        return True
    return False


# def validate_file_encoding(file_content, expected_encoding):
#    result = chardet.detect(file_content)
#    if result["encoding"] == expected_encoding:
#        return True
#    return False


def normalize_headers(file_content, delimeter):
    lines = file_content.split("\n")
    headers = lines[0].strip().split(delimeter)
    normalized_headers = [
        re.sub(r"[^a-zA-Z0-9_ ]", "", header)
        .replace(" ", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("-", "_")
        .lower()
        for header in headers
    ]
    lines[0] = delimeter.join(normalized_headers)
    return "\n".join(lines)


def validate_file_number_of_columns(file_content, expected_columns_count, delimeter):
    columns_count = len(file_content.split("\n")[0].split(delimeter))
    if columns_count == expected_columns_count:
        return True
    return False


def validate_file_columns_names(file_content, expected_columns_names, delimeter):
    columns_names = file_content.split("\n")[0].strip().split(delimeter)
    expected_columns_names = [col["S"] for col in expected_columns_names]
    if columns_names == expected_columns_names:
        return True
    return False


def add_date_column(bucket_name, prefix, column_name, delimeter):
    obj = s3.get_object(Bucket=bucket_name, Key=prefix)
    file_content = obj["Body"].read()
    df = pd.read_csv(io.BytesIO(file_content))
    file_name = prefix.rsplit("/", 1)[-1]
    date_string = file_name.split("_")[-1].split(".")[0]
    date = datetime.strptime(date_string, "%Y%m%d").date()
    df[column_name] = date
    return df


def transform_to_parquet(df, bucket_name, prefix):
    file_name = prefix.rsplit("/", 1)[-1]
    parquet_file_name = file_name.split(".")[0] + ".parquet"
    buffer = io.BytesIO()
    df.to_parquet(buffer)
    buffer.seek(0)
    s3.upload_fileobj(buffer, bucket_name, "staging-zone/" + parquet_file_name)


def lambda_handler(event, context):

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
            valid_file = True
    if valid_file:
        response = table.get_item(Key={"document_key": district_key})
        district_rules = response.get("Item")

        if "file_extension" in district_rules["validation_rules"]:
            extension_status = validate_file_extension(
                file_name, district_rules["validation_rules"].get("file_extension")
            )
            print(f"extension_status: {extension_status}")
        obj = s3.get_object(Bucket=bucket_name, Key=prefix)
        file_content = obj["Body"].read()
        encoding = district_rules["validation_rules"].get("encoding")
        if "encoding" in district_rules["validation_rules"] and extension_status:
            # encoding_status = validate_file_encoding(file_content, encoding)
            # print(f"encoding_status: {encoding_status}")
            encoding_status = True
        if encoding_status:
            delimeter = district_rules["validation_rules"].get("delimeter")
            file_content = file_content.decode(encoding)
            file_content = normalize_headers(file_content, delimeter)
            if "columns_count" in district_rules["validation_rules"]:
                number_of_columns_status = validate_file_number_of_columns(
                    file_content,
                    district_rules["validation_rules"].get("columns_count"),
                    delimeter,
                )
                print(f"number_of_columns_status: {number_of_columns_status}")
            if "columns_names" in district_rules["columns_names"]:
                columns_names_status = validate_file_columns_names(
                    file_content,
                    district_rules["validation_rules"].get("columns_names"),
                    delimeter,
                )
                print(f"columns_names_status: {columns_names_status}")

        sns_topic_name = "_".join(document_key.split("/")).lower()
        topic_arn = get_topic_arn(sns_topic_name)
        if topic_arn:
            valid_sns_topic = True

    # if valid_sns_topic:
    #    response = sns.publish(
    #        TopicArn=topic_arn, Message=f"New file '{file_name}' at '{document_key}'"
    #    )
