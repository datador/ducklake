import os
from pathlib import Path
import duckdb

def _load_env(env_path: str | None = None) -> None:
    """
    Load a .env file. If env_path is None:
      1) try CWD/.env
      2) try CWD/../.env
    """
    candidates = []
    if env_path:
        candidates = [Path(env_path)]
    else:
        cwd = Path.cwd()
        candidates = [cwd / ".env", cwd.parent / ".env"]

    for p in candidates:
        if p.exists():
            for line in p.read_text().splitlines():
                s = line.strip()
                if s and "=" in s and not s.startswith("#"):
                    k, v = s.split("=", 1)
                    os.environ[k.strip()] = v.strip()
            return

def connect_and_attach(env_path: str | None = None):
    """
    One-liner for notebooks:
      - Loads .env (repo root) unless env already set
      - Loads DuckLake + httpfs as needed
      - ATTACHes the lake using catalog metadata (no DATA_PATH override)
    Returns a ready duckdb.Connection.
    """
    _load_env(env_path)

    pg = (
        f"host=localhost "
        f"port={os.environ.get('POSTGRES_PORT','5432')} "
        f"dbname={os.environ.get('POSTGRES_DB','ducklake_catalog')} "
        f"user={os.environ.get('POSTGRES_USER','ducklake')} "
        f"password={os.environ.get('POSTGRES_PASSWORD','ducklake')}"
    )
    lake = os.environ.get("DUCKLAKE_NAME", "my_lake")
    storage = os.environ.get("DL_STORAGE", "local").lower()

    con = duckdb.connect(":memory:")
    con.execute("INSTALL ducklake; LOAD ducklake;")

    if storage == "s3":
        con.execute("INSTALL httpfs; LOAD httpfs;")
        endpoint = os.environ.get("DL_S3_HOST_ENDPOINT", "localhost:9000")
        endpoint = endpoint.replace("http://", "").replace("https://", "")
        region   = os.environ.get("DL_S3_REGION", "us-east-1")
        urlstyle = os.environ.get("S3_URL_STYLE", "path")
        usessl   = str(os.environ.get("S3_USE_SSL", "false")).lower() in ("1","true","yes")

        ak = os.environ.get("MINIO_ROOT_USER", "minioadmin")
        sk = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

        con.execute("SET s3_endpoint=?", [endpoint])
        con.execute("SET s3_region=?",   [region])
        con.execute(f"SET s3_url_style='{urlstyle}'")
        con.execute(f"SET s3_use_ssl={'true' if usessl else 'false'}")
        con.execute("SET s3_access_key_id=?",     [ak])
        con.execute("SET s3_secret_access_key=?", [sk])
    else:
        # ensure 'data/' resolves for local mode if notebook runs inside notebooks/
        repo_root = Path.cwd()
        if not (repo_root / "data").exists() and (repo_root.parent / "data").exists():
            os.chdir(repo_root.parent)

    # attach using catalog only; let DuckLake resolve DATA_PATH from metadata
    con.execute(f"ATTACH 'ducklake:postgres:{pg}' AS {lake};")
    con.execute(f"USE {lake};")
    return con