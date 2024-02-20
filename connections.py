from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from datetime import timedelta
from couchbase.options import ClusterOptions
import requests
from requests.auth import HTTPBasicAuth

def get_connection(ip, username='admin', password='Cohe$1ty'):
    # Connect options - authentication
    auth = PasswordAuthenticator(
        username,
        password,
    )

    # Get a reference to our cluster
    # NOTE: For TLS/SSL connection use 'couchbases://<your-ip-address>' instead
    conn = Cluster(f'couchbase://{ip}', ClusterOptions(auth))

    conn.wait_until_ready(timedelta(seconds=5))

    return conn

def get_cluster_nodes(ip, port=8091, username='admin', password='Cohe$1ty'):
    url = f'http://{ip}:{port}/pools/default/'
    res = requests.get(url, auth=HTTPBasicAuth(username, password)).json().get('nodes')
    result = []
    for node in res:
        result.append(node.get('hostname').split(':')[0])
    return result

if __name__ == '__main__':
    # conn = get_connection('10.14.69.126')
    result = get_cluster_nodes('10.14.69.126')
    print(result)