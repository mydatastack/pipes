import boto3
import os
from urllib.parse import unquote
from collections import namedtuple
import asyncio
import pprint
import json
from itertools import groupby
import pandas as pd
from pandas.io.json import json_normalize
from datetime import datetime
import time


S3Object = namedtuple('S3MetaData', ('bucket', 'key'))



def configure_s3_aws_client():
    if os.environ.get('AWS_DEV') is not None:
        return boto3.resource(service_name='s3', endpoint_url='http://localhost:4572')
    else:
        return boto3.resource(service_name='s3')


async def get_content_from_s3_object(bucket: str, key: str, s3)->list:
    try:
        return [json.loads(line) for line in s3.Object(bucket, key).get()['Body'].read().decode('utf-8').split('\n') if line]
    except Exception as e:
        print(e)
        return []


async def get_content_from_s3_object_async(bucket: str, key: str, s3):
    return await asyncio.create_task(get_content_from_s3_object(bucket, key, s3))

async def save_file_as_parquet(s3, name: str, data: pd.DataFrame):
    #f's3://demo-bucket/{name}/1.parquet'
    s3.Object('test-tarasowski-ga', f'{name}/1.json').put(Body=data.reset_index().to_json(orient='records'))
    #print(data.reset_index().to_json(orient='records'))
    print('stored')


async def handler_async(event):
    s3 = configure_s3_aws_client()
    data = [S3Object(record['s3']['bucket']['name'], unquote(record['s3']['object']['key'])) for record in
            list(event['Records'])]

    tasks = [get_content_from_s3_object_async(d.bucket, d.key, s3) for d in data]
    print('-----------------------------------')
    print('Total tasks : ' + len(tasks).__str__())
    task_results = await asyncio.gather(*tasks)
    data = [item for sublist in task_results for item in sublist]
    print("Total rows : " + len(data).__str__())
    print('-----------------------------------')

    data = [json_normalize(d) for d in data]
    df = pd.concat(data, sort=True)#.drop_duplicates() #pd.DataFrame(json_normalize(data, errors='ignore')).drop_duplicates()
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
    #     print(df.columns)



    df['date_time'] = df.received_at_apig.apply(lambda e: datetime.utcfromtimestamp(int(int(e) / 1000 )))
    df['year'] = df.date_time.apply(lambda e: e.year)
    df['month'] = df.date_time.apply(lambda e: e.month)
    df['day'] = df.date_time.apply(lambda e: e.day)
    df['hour'] = df.date_time.apply(lambda e: e.hour)

    grouped = df.groupby(['body.t', 'year', 'month', 'day', 'hour'])
    tasks = [save_file_as_parquet(s3, f'processed/system_source=ga/event_type={k[0]}/year={k[1]}/month={k[2]}/day={k[3]}/hour={k[4]}', v) for k, v in grouped]
    await asyncio.gather(*tasks)


# Steps to process
# 1. parse payloda
def handler(event, ctx):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(handler_async(event))
    # for bucket in s3.buckets.all():
    #     print(bucket)
