import json
from functools import partial, reduce
from collections import namedtuple
from urllib.parse import unquote
from itertools import groupby
import boto3

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
            (el['body']['ds'], el['body']['t'], el) 
            for el in data
           ]

def group_by_ds(data: list) -> list:
    return [
            (ds, event, list(dt for ds, ev, dt in data)) 
            for ds, g1 in groupby(data, key=lambda x: x[0])
            for event, data in groupby(g1, key=lambda x: x[1])
            ]

def sort_data(data: list) -> list:
    return sorted(data, key=lambda t: (t[0],t[1])) 

def construct_keys(event: dict, data: list):
     keys = pipe(
                get_records,
                get_list,
                get_s3metadata,
            ) (event)          
     bucket = keys[0].bucket
     folders = keys[0].key.split('/')
     base_folders = "/".join(folders[:2])
     partition_folders = "/".join(folders[2:5])
     return [
             (bucket, base_folders + '/' + ds + '/' + event + '/' + partition_folders, ds, event, body) 
             for ds, event, body in data
            ]
   

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
               (lambda x: print(json.dumps(x, indent=4, separators=(',',':')))),
                #merge_tuples,
                #(lambda x: print(x))
               )(event)
