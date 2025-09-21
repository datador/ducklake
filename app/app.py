import os
import time
import duckdb

DL_STORAGE = os.environ.get("DL_STORAGE", "local").lower()       # "s3" | "local"
PG_CONN    = os.environ["DL_PG_CONN"]
LAKE_NAME  = os.environ.get("DUCKLAKE_NAME", "my_lake")

con = duckdb.connect(":memory:")
con.execute("INSTALL ducklake; LOAD ducklake;")
con.execute("INSTALL postgres; LOAD postgres;")
con.execute("INSTALL httpfs;  LOAD httpfs;")

def ensure_minio_bucket():
    from minio import Minio
    endpoint = os.environ.get("S3_ENDPOINT", "minio:9000").replace("http://","").replace("https://","")
    access   = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("MINIO_ROOT_USER", "minioadmin")
    secret   = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
    mc = Minio(endpoint, access_key=access, secret_key=secret, secure=False)
    bucket = os.environ["DL_S3_BUCKET"]
    if not mc.bucket_exists(bucket):
        mc.make_bucket(bucket)

def attach_lake():
    if DL_STORAGE == "s3":
        ensure_minio_bucket()
        endpoint = os.environ.get("S3_ENDPOINT", "minio:9000").replace("http://","").replace("https://","")
        region   = os.environ.get("S3_REGION",   "us-east-1")
        urlstyle = os.environ.get("S3_URL_STYLE","path").lower()
        usessl   = os.environ.get("S3_USE_SSL",  "false").lower() in ("1","true","yes")

        access = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("MINIO_ROOT_USER", "minioadmin")
        secret = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

        con.execute("SET s3_endpoint=?", [endpoint])
        con.execute("SET s3_region=?",   [region])
        con.execute(f"SET s3_url_style='{urlstyle}'")
        con.execute(f"SET s3_use_ssl={'true' if usessl else 'false'}")
        con.execute("SET s3_access_key_id=?",     [access])
        con.execute("SET s3_secret_access_key=?", [secret])

        bucket = os.environ["DL_S3_BUCKET"]
        prefix = os.environ.get("DL_S3_PREFIX", "main").strip("/")
        data_path  = f"s3://{bucket}/{prefix}/"
        attach_opts = f"(DATA_PATH '{data_path}')"
    else:
        data_rel = os.environ.get("DL_DATA_PATH", "data/").rstrip("/") + "/"
        data_path  = data_rel
        attach_opts = f"(DATA_PATH '{data_rel}')"

    attach_sql = f"ATTACH 'ducklake:postgres:{PG_CONN}' AS {LAKE_NAME} {attach_opts};"

    for i in range(30):
        try:
            con.execute(attach_sql)
            break
        except Exception:
            if i == 29: 
                raise
            time.sleep(2)

    con.execute(f"USE {LAKE_NAME};")
    return data_path

def print_status(data_path):
    print("== DuckLake Bootstrap ==")
    print("Lake name:   ", LAKE_NAME)
    print("Storage:     ", DL_STORAGE)
    print("DATA_PATH:   ", data_path)

    try:
        row = con.execute(f"SELECT id FROM {LAKE_NAME}.last_committed_snapshot();").fetchone()
        last_id = row[0] if row else None
        print("\nLast committed snapshot:", last_id)
    except Exception as e:
        print("\nLast committed snapshot: <unavailable>", e)

    # list schemas & table inventory
    schemas = con.execute(
        "SELECT schema_name FROM information_schema.schemata ORDER BY 1;"
    ).fetchdf()
    print("\nSchemas:\n", schemas)

    tables = con.execute(
        "SELECT table_schema, table_name "
        "FROM information_schema.tables "
        "ORDER BY 1, 2;"
    ).fetchdf()
    print("\nTables:\n", tables)


if __name__ == "__main__":
    path = attach_lake()
    print_status(path)