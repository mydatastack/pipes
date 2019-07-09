import json
from functools import partial, reduce
from collections import namedtuple
from urllib.parse import unquote
import sys
import boto3
s3 = boto3.resource('s3')
pipe = lambda *args: lambda x: reduce(lambda a, fn: fn(a), args, x)

S3Object = namedtuple('S3MetaData', ('bucket', 'key'))


get_records = lambda e: e['Records']
get_list = lambda e: map(lambda e: e['s3'], e)
get_s3metadata = lambda ev: map(lambda el: S3Object(el['bucket']['name'], unquote(el['object']['key'])), ev)

def load_file(m):
    try:
        print(m.bucket)
        print(m.key)
        obj = s3.Object(m.bucket, m.key)
        print(obj.get()['Body'].read().decode('utf-8'))
        return obj
    except Exception as e:
        print(e) 

log = lambda desc: lambda value: print(desc + ': ' + json.dumps(list(value))) or value

def handler(event, ctx):
    return pipe( 
                get_records,
                get_list,
                get_s3metadata,
                (lambda o: map(load_file,o)),
                log('after map'),
                (lambda x: x),
                (lambda x: x) 
               )(event)
