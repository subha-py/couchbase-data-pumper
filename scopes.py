from connections import get_connection
from buckets import get_bucket
import requests
from requests.auth import HTTPBasicAuth

from couchbase.exceptions import  ScopeAlreadyExistsException
def create_scope(bucket, scope_name):
    collection_manager = bucket.collections()
    try:
        collection_manager.create_scope(scope_name)
        print(f'scope with {scope_name} created!')
    except ScopeAlreadyExistsException:
        print(f'Scope {scope_name} already exists! Not creating a new one')


def get_scope(bucket, scope_name):
    return bucket.scope(scope_name)

def get_scope_details(scope,ip,port='8091', username='admin', password='Cohe$1ty'):
    bucket_name = scope._bucket.name
    url = f'http://{ip}:{port}/pools/default/buckets/{bucket_name}/scopes/'
    res = requests.get(url, auth=HTTPBasicAuth(username, password), ).json()
    for s in res.get('scopes'):
        if scope.name == s.get('name'):
             return s
    return None

def get_collection_ids_from_scope(scope, ip, port='8091', username='admin', password='Cohe$1ty'):
    result = get_scope_details(scope, ip, port, username, password)
    collection_ids = []
    for c in result.get('collections'):
        c.get('uid')
        coll_id = f"0x{c.get('uid')}"
        collection_ids.append(coll_id)
    return collection_ids

def create_multi_scope(bucket ,prefix, count, start=0):
    for i in range(start, count):
        name = f'{prefix}{i}'
        create_scope(bucket, name)

def get_all_scopes(bucket):
    return bucket.collections().get_all_scopes()

if __name__ == '__main__':
    ip = '10.3.59.180'
    conn = get_connection(ip)
    bucket = get_bucket(conn, 'st-vmrobo')
    create_multi_scope(bucket, 'st-scope', 999, 855)
    print('Success!')