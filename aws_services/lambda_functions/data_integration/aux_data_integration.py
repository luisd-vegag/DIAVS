import re
from datetime import datetime

# Packages from layers
import cchardet
import pandas as pd


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


def validate_file_encoding(file_bytes, expected_encoding):
    """
    This function validates the encoding of a file.
    It takes the file content and the expected encoding as input and
    returns True if the file encoding matches the expected encoding.
    """
    result = cchardet.detect(file_bytes)
    print(f"Incoming encoding: {result}")
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
        re.sub(r"[^a-zA-Z0-9_ -]", "", header)
        .lstrip()
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


def add_date_columns(df, date_details, file_name, output_base_file_name):
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
        source_date = datetime.strptime(
            source_date, date_details["source_date"]["date_format"])
        df["source_date"] = source_date.strftime("%Y-%m-%d")
        df["source_date"] = pd.to_datetime(
            df["source_date"], format="%Y-%m-%d")
        output_base_file_name = source_date.strftime(
            "%Y%m%d") + "_" + output_base_file_name

    if "file_date" in date_details and date_details["file_date"] == True:
        now = datetime.now()
        df["file_date"] = pd.to_datetime(
            now.strftime("%Y-%m-%d"), format="%Y-%m-%d")
        output_base_file_name = output_base_file_name + \
            "_" + now.strftime("%Y%m%d")
    output_file_name = output_base_file_name
    return df, output_file_name


def get_record_delimiter(file_extension, encoding):
    if file_extension == 'csv':
        return '\n'
    elif file_extension == "tsv":
        return "\t"
    elif file_extension == 'txt':
        if encoding == 'utf-8':
            return '\n'
        elif encoding == 'utf-16':
            return '\r\n'
    # Not sure about this one
    elif file_extension == 'xls' or file_extension == 'xlsx' or file_extension == 'xlsb' or file_extension == 'xlsm':
        return '\r\n'
    else:
        return None
