import json
from functools import partial, reduce
from collections import namedtuple, defaultdict
from urllib.parse import unquote
from itertools import chain, groupby
from operator import itemgetter
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
        obj = s3.Object(m.bucket, m.key)
        return obj.get()['Body'].read().decode('utf-8')
    except Exception as e:
        print(e) 

log = lambda desc: lambda value: print(desc + ': ' + json.dumps(list(value))) or value

splitStr = lambda x: x.split('\n') 

def group_source_events(entry_lst):
   return  [(ds, evt, d) for entry in entry_lst]

def handler(event, ctx):
    return pipe( 
                get_records,
                get_list,
                get_s3metadata,
                (lambda o: map(load_file, o)),
                (lambda o: map(splitStr, o)),
                (lambda o: chain.from_iterable(o)), 
                (lambda o: filter(lambda x: bool(x), o)),
                (lambda o: map(lambda x: json.loads(x), o)),
                (lambda o: map(lambda x: (x['body']['ds'], x['body']['t'],  x), o)),
                group_source_events,
                (lambda x: print(json.dumps(list(x), indent=4, separators=(',',':')))) 
               )(event)
