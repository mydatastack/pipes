import json
from functools import partial, reduce
from collections import namedtuple
from urllib.parse import unquote
from itertools import groupby
import boto3
import time

s3 = boto3.resource('s3')
pipe = lambda *args: lambda x: reduce(lambda a, fn: fn(a), args, x)

S3Object = namedtuple('S3MetaData', ('bucket', 'key'))


def get_records(event: dict) -> list: 
    return event['Records']

def get_list(event: list) -> list:
    return [
            el['s3'] 
            for el in event
           ]

def get_s3metadata(s3meta: list) -> S3Object: 
    return [
            S3Object(el['bucket']['name'], unquote(el['object']['key'])) 
            for el in s3meta
           ]

def load_file(m: list) -> str:
    try:
        obj = s3.Object(m[0].bucket, m[0].key)
        return obj.get()['Body'].read().decode('utf-8')
    except Exception as e:
        print(e) 
        return e


def split_str(data: str) -> list: 
    return data.split('\n') 

def remove_new_lines(data: list) -> list:
    return [
            line 
            for line in data
            if line
           ]

def decode_json(data: list) -> list:
    return [
            json.loads(line)
            for line in data
           ]

def take_props(data: list) -> list:
    return [
            (el['body']['tid'].lower(), el['body']['ds'], el['body']['t'], el) 
            for el in data
           ]

def group_by_ds(data: list) -> list:
    return [
            (tid, ds, event, list(dt for tid, ds, ev, dt in data)) 
            for tid, g1 in groupby(data, key=lambda x: x[0])
            for ds, g2 in groupby(g1, key=lambda x: x[1])
            for event, data in groupby(g2, key=lambda x: x[2])
            ]

def sort_data(data: list) -> list:
    return sorted(data, key=lambda t: (t[0],t[1],t[2])) 

def construct_keys(event: dict, data: list) -> list:
     keys = pipe(
                get_records,
                get_list,
                get_s3metadata,
            ) (event)          
     bucket = keys[0].bucket
     folders = keys[0].key.split('/')
     base_folders = "/".join(folders[:2])
     partition_folders = "/".join(folders[2:6])
     return [
             (bucket, tid, base_folders + '/' + tid + '/' + ds + '/' + event + '/' + partition_folders, ds, event, body) 
             for tid, ds, event, body in data
            ]

def save_to_s3(data: list, ts=time.strftime("%Y-%m-%dT%H:%M:%S%z", time.gmtime())) -> str:
    try:
        for bucket, tid, folder, ds, event, body in data:
            key = f'{tid}-{event}-{ts}'
            body_json = [json.dumps(record) for record in body]
            new_line_delimited = '\n'.join(body_json)
            s3.Object(bucket, folder + '/' + key).put(Body=new_line_delimited)
        return 'success'
    except Exception as e:
        print(e) 
        return e

def handler(event, ctx):
    return pipe( 
                get_records,
                get_list,
                get_s3metadata,
                load_file,
                split_str,
                remove_new_lines,
                decode_json,
                take_props,
                sort_data,
                group_by_ds,
                partial(construct_keys, event),
                save_to_s3,
               (lambda x: print(json.dumps(x, indent=4, separators=(',',':'))) or x),
               )(event)
