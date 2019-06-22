import base64
import json
import ast
from urllib.parse import parse_qsl
import datetime

    
def destructure(data):
    entry = ast.literal_eval(data.decode('utf-8'))
    entrydict = dict(entry)
    body = entry['body']
    parsed = parse_qsl(body)
    bodydict = {'body': dict(parsed)}
    final = {**entrydict, **bodydict, 'received_at_frh': datetime.datetime.now().isoformat() }
    return final

def main(event, ctx):
    rs = event['records']
    decoded_data = map(lambda x: {
        'recordId': x['recordId'],
        'result': 'Ok',
        'data': base64.b64decode(x['data'])
    }, rs)
    splitted_data = map(lambda x: {
        'recordId': x['recordId'],
        'result': 'Ok',
        'data': destructure(x['data'])
        }, decoded_data)
    convert_bytes = map(lambda x: {
        'recordId': x['recordId'],
        'result': 'Ok',
        'data': base64.b64encode((json.dumps(x['data']) + '\n').encode('utf-8')).decode('utf-8')
        }, splitted_data)
    return {'records': list(convert_bytes)} 
