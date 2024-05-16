from couchbase.management.options import CreatePrimaryQueryIndexOptions


def get_bucket(conn, bucket_name):
    # ixm = conn.query_indexes()
    # ixm.create_primary_index(bucket_name, CreatePrimaryQueryIndexOptions(ignore_if_exists=True))
    return conn.bucket(bucket_name)

