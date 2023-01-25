import boto3
import json
import re
import os
import io
from datetime import datetime
import tempfile

# Packages from layers
import cchardet
import pandas as pd


REGION = os.getenv("REGION")
INPUT_RAW_BUCKET = os.getenv("INPUT_RAW_BUCKET")
RAW_ZONE_BUCKET = os.getenv("RAW_ZONE_BUCKET")
LANDING_ZONE_BUCKET = os.getenv("LANDING_ZONE_BUCKET")
STAGING_ZONE_BUCKET = os.getenv("STAGING_ZONE_BUCKET")
ERROR_ZONE_BUCKET = os.getenv("ERROR_ZONE_BUCKET")
s3_client = boto3.client("s3")
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
    print("HOLA 13")
    df = pd.read_csv(io.BytesIO(bytes(file_content, encoding)),
                     dtype=dtypes, delimiter=delimiter)
    print("HOLA 14")
    for col in columns_details:
        if col["data_type"] == "date" and "date_format" in col:
            df[col["header"]] = pd.to_datetime(
                df[col["header"]], format=col["date_format"])
    print("HOLA 15")

    return df


def add_date_columns(df, date_details, file_name, output_base_file_name):
    """
    This function adds date columns to a pandas dataframe.
    It takes the dataframe, the date details and the file name as input and
    returns the dataframe with additional date columns.
    """

    if "parameter_date" in date_details:
        df["parameter_date"] = pd.to_datetime(
            df[date_details["parameter_date"]], format="%Y-%m-%d")
        print("HOLA 17")
    if "source_date" in date_details:
        source_date = re.search(
            date_details["source_date"]["date_regex"], file_name).group()
        source_date = datetime.strptime(
            source_date, date_details["source_date"]["date_format"])
        df["source_date"] = source_date.strftime("%Y-%m-%d")
        df["source_date"] = pd.to_datetime(
            df["source_date"], format="%Y-%m-%d")
        output_base_file_name = source_date.strftime(
            "%Y%m%d") + "_" + output_base_file_name
        print("HOLA 18")

    if "file_date" in date_details and date_details["file_date"] == True:
        now = datetime.now()
        df["file_date"] = pd.to_datetime(
            now.strftime("%Y-%m-%d"), format="%Y-%m-%d")
        output_base_file_name = output_base_file_name + \
            "_" + now.strftime("%Y%m%d")
        print("HOLA 19")
    return df, output_base_file_name


def write_df_to_s3_parquet(df, output_bucket_name, output_prefix):
    """
    This function writes a dataframe to S3 in parquet format.
    The dataframe is written to a temporary file in the local filesystem, 
    and then uploaded to S3.

    Parameters:
        df (pandas.DataFrame): The dataframe to be written to S3.
        output_bucket_name (str): The name of the S3 bucket to which the dataframe will be written.
        output_prefix (str): The name of the output file in S3.

    Returns:
        None
    """
    print("HOLA 21")
    # Create a temporary file
    with tempfile.NamedTemporaryFile() as temp:
        print("HOLA 22")
        # Write the dataframe to the temporary file in parquet format
        df.to_parquet(temp.name, index=False, compression='snappy')
        print("HOLA 23")
        # Move the file pointer to the beginning of the file
        temp.seek(0)
        print("HOLA 24")
        # Upload the file to S3
        output_prefix = output_prefix + ".parquet"
        s3_client.upload_file(temp.name, output_bucket_name, output_prefix)
        print("HOLA 25")


def lambda_handler(event, context):
    """
    This function is the entry point for the Lambda function.
    It takes the event and the context as input and
    performs validation and processing on the file in S3.
    """
    input_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
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
            output_base_file_name = district_data["output_base_file_name"]
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
        print("HOLA 1")
        obj = s3_client.get_object(Bucket=input_bucket_name, Key=prefix)
        print("HOLA 2")
        file_content = obj["Body"].read()
        print("HOLA 3")
        encoding = district_rules["validation_rules"].get("encoding")
        if "encoding" in district_rules["validation_rules"] and extension_status:
            encoding_status = validate_file_encoding(file_content, encoding)
            print("HOLA 4")
            print(f"encoding_status: {encoding_status}")
        if encoding_status:
            delimiter = district_rules["validation_rules"].get("delimiter")
            print("HOLA 5")
            file_content = file_content.decode(encoding)
            print("HOLA 6")
            file_content = normalize_headers(file_content, delimiter)
            print("HOLA 7")
            if "columns_count" in district_rules["validation_rules"]:
                print("HOLA 8")
                number_of_columns_status = validate_file_number_of_columns(
                    file_content,
                    district_rules["validation_rules"].get("columns_count"),
                    delimiter,
                )
                print("HOLA 9")
                print(f"number_of_columns_status: {number_of_columns_status}")

            if "columns_details" in district_rules["validation_rules"]:
                columns_details = district_rules["validation_rules"].get(
                    "columns_details")
                print("HOLA 10")
                columns_names_status = validate_file_columns_names(
                    file_content,
                    columns_details,
                    delimiter,
                )
                print("HOLA 11")
                print(f"columns_names_status: {columns_names_status}")
                if "date_details" in district_rules["validation_rules"]:
                    date_details = district_rules["validation_rules"].get(
                        "date_details")
                print("HOLA 12")
                df = file_content_to_df(
                    file_content, columns_details, delimiter, encoding)
                print("HOLA 16")
                df, output_file_name = add_date_columns(
                    df, date_details, file_name, output_base_file_name)
                output_prefix = prefix.rsplit(
                    "/", 1)[0] + "/" + output_file_name
                output_bucket_name = re.sub(
                    INPUT_RAW_BUCKET, STAGING_ZONE_BUCKET, input_bucket_name)
                print(output_file_name)
                print("HOLA 20")

                write_df_to_s3_parquet(
                    df, output_bucket_name, output_prefix)

        sns_topic_name = "_".join(document_key.split("/")).lower()
        topic_arn = get_topic_arn(sns_topic_name)
        if topic_arn:
            valid_sns_topic = True

    # if valid_sns_topic:
    #    response = sns.publish(
    #        TopicArn=topic_arn, Message=f"New file '{file_name}' at '{document_key}'"
    #    )
