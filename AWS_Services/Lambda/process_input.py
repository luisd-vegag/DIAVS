import csv
import json
import pandas as pd
import boto3
import botocore.response

import re
import os
import io
from datetime import datetime
import tempfile

# Packages from layers
import cchardet
import pandas as pd

MINIMUN_REMAINING_TIME_MS = 500
ROWS_PER_LAMBDA = 2500


def lambda_handler(event, context, offset=0, fieldnames=None, encoding='utf-8', delimiter=','):
    input_bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    prefix = event["Records"][0]["s3"]["object"]["key"]
    s3_resource = boto3.resource('s3')
    s3_object = s3_resource.Object(bucket_name=input_bucket_name, key=prefix)
    bodylines = get_object_bodylines(s3_object, offset, encoding)
    csv_reader = csv.DictReader(
        bodylines[0], fieldnames=fieldnames, delimiter=delimiter)
    rows = []
    for row in csv_reader:
        rows.append(row)
        if len(rows) >= ROWS_PER_LAMBDA or context.get_remaining_time_in_millis() < MINIMUN_REMAINING_TIME_MS:
            print(
                f"Start Time per lambda: {context.get_remaining_time_in_millis()}")
            df = pd.DataFrame(rows)
            print(df.head(3))
            # process df here
            print(
                f"Snd Time per lambda: {context.get_remaining_time_in_millis()}")
            rows = []
    new_offset = offset + bodylines[1]
    if new_offset < s3_object.content_length:
        new_event = {
            **event,
            "offset": new_offset,
            "fieldnames": fieldnames,
            "encoding": encoding,
            "delimiter": delimiter
        }
        invoke_lambda(context.function_name, new_event)
    return


def invoke_lambda(function_name, event):
    payload = json.dumps(event).encode('utf-8')
    client = boto3.client('lambda')
    response = client.invoke(
        FunctionName=function_name,
        InvocationType='Event',
        Payload=payload
    )


def get_object_bodylines(s3_object, offset, encoding):
    resp = s3_object.get(Range=f'bytes={offset}-')
    body: botocore.response.StreamingBody = resp['Body']
    offset = 0
    lines = []
    pending = b''
    for chunk in body.iter_chunks(1024):
        lines_in_chunk = (pending + chunk).splitlines(True)
        lines += map(lambda x: x.decode(encoding), lines_in_chunk[:-1])
        offset += sum(map(lambda x: len(x), lines_in_chunk[:-1]))
        pending = lines_in_chunk[-1]
    if pending:
        lines.append(pending.decode(encoding))
        offset += len(pending)
    return (lines, offset)
