import subprocess
import argparse
import concurrent.futures
from itertools import cycle

from connections import get_connection, get_cluster_nodes
from buckets import get_bucket
from scopes import get_scope, get_collection_ids_from_scope, get_all_scopes



parser = argparse.ArgumentParser(description='A program to populate multiple collections in couchbase',
                                 usage='python3 pumper.py --python_path=<> --cbworkloadgen_path=<>  ')

parser.add_argument('--python_path', default='/home/oracle/Downloads/couchbase/couchbase-cli/venv/bin/python', help='python path of couchbase-cli (default:/home/oracle/Downloads/couchbase/couchbase-cli/venv/bin/python)', type=str)
parser.add_argument('--cbworkloadgen_path', default='/home/oracle/Downloads/couchbase/couchbase-cli/cbworkloadgen', help='path where cbworkloadgen resides (default: /home/oracle/Downloads/couchbase/couchbase-cli/cbworkloadgen)', type=str)
parser.add_argument('--node', default='127.0.0.1', help='ip of couchbase server (default: 127.0.01)', type=str)
parser.add_argument('--port', default='8091', help='port of couchbase server (default: 8091)', type=str)
parser.add_argument('--username', default='admin', help='username (default: admin)', type=str)
parser.add_argument('--password', default='Cohe$1ty', help='password (default: Cohe$1ty)', type=str)
parser.add_argument('--bucket', default='st-vmrobo', help='bucket (default: st-vmrobo)', type=str)
parser.add_argument('--scope', default='st-scope', help='scope (default: st-scope)', type=str)
parser.add_argument('--threads', default='1', help='threads (default: 9)', type=str)
parser.add_argument('--items', default='1000', help='number of documents to be inserted per collection (default: 100)', type=str)
parser.add_argument('--prefix', default='st', help='document id prefix (default: st)', type=str)
parser.add_argument('--size', default='10000', help='size of each document in bytes (default: 100)', type=str)

def pump_data(pythonpath, cbworkloadgen_path, node, port, username, password, bucket, threads, items, prefix, size, collection):
    cmd = f'{pythonpath} {cbworkloadgen_path} -n {node}:{port} -u {username} -p {password} -b {bucket} --no-ssl-verify -t {threads} -r 1.0 -i {items} --prefix {prefix} -s {size} -c {collection} --json'
    cmd_args = cmd.split()
    exit_code = subprocess.run(cmd_args, stdout=subprocess.DEVNULL)
    if exit_code.returncode == 0:
        print(f'cbworkloadgen completed with - {cmd}')
        return True
    return False

def pump_data_in_parallel(python_path, cbworkloadgen_path, node, port, username, password, bucket, threads, items, prefix, size, collection_ids):
    future_to_coll = {}
    node_cycle = cycle(get_cluster_nodes(node))
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(collection_ids), 512)) as executor:
        for c in collection_ids:
            arg = (python_path, cbworkloadgen_path, next(node_cycle), port, username, password, bucket, threads, items, prefix, size, c)
            future_to_coll[executor.submit(pump_data, *arg)] = c

    result = []
    for future in concurrent.futures.as_completed(future_to_coll):
        c = future_to_coll[future]
        try:
            res = future.result()
            if not res:
                result.append(c)
        except Exception as exc:
            print("%r generated an exception: %s" % (c, exc))
    return result

if __name__ == '__main__':
    result = parser.parse_args()
    conn = get_connection(result.node)
    b = get_bucket(conn, result.bucket)
    scope_objs = get_all_scopes(b)
    collection_ids = []
    for scope in scope_objs:
        scope_obj = get_scope(b, scope.name)
        print(f'getting collection ids for  - {scope.name}')
        collection_ids += get_collection_ids_from_scope(scope_obj, result.node)
    print(collection_ids)
    pump_data_in_parallel(result.python_path, result.cbworkloadgen_path, result.node, result.port, result.username,
                          result.password, result.bucket, result.threads, result.items, result.prefix, result.size,
                          collection_ids)
