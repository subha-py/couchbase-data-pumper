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
    return res

if __name__ == '__main__':
    ip = '10.14.69.126'
    conn = get_connection(ip)
    bucket = get_bucket(conn, 'st-vmrobo')
    # create_scope(bucket, 'st-scope')
    scope = get_scope(bucket, 'st-scope')
    result = get_scope_details(scope, ip)
    print('Success!')