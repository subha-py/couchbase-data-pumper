from couchbase.management.collections import CollectionSpec
from couchbase.exceptions import CollectionAlreadyExistsException

from connections import get_connection
from buckets import get_bucket
from scopes import get_scope, get_all_scopes
def create_collection(scope, collection_name):
    coll_manager = scope._bucket.collections()
    try:
        coll_manager.create_collection(scope.name, collection_name)
        print(f'{collection_name} successfully created!')
    except CollectionAlreadyExistsException:
        print(f'collection - {collection_name} already exists!')

def get_collection(scope, collection_name):
    return scope.collection(collection_name)

def create_multi_collection(scope, prefix, count, start=0):
    for i in range(count):
        name = f'{prefix}{i}'
        create_collection(scope, name)

if __name__ == '__main__':
    conn = get_connection('10.3.59.181')
    bucket = get_bucket(conn, 'st-vmrobo0')
    count = 1
    scope_objs = get_all_scopes(bucket)
    for scope in scope_objs:
        scope_obj = get_scope(bucket, scope.name)
        create_multi_collection(scope_obj, 'stcollection', 100)
    print('Success!')