from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from datetime import timedelta
from couchbase.options import ClusterOptions


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







def get_collection(scope_obj, collection):
    return scope_obj.collection(collection)

if __name__ == '__main__':
    conn = get_connection('10.14.69.126')