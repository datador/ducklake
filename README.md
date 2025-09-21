# DuckLake Demo — Postgres Catalog + MinIO (eða Local) + Sýnidæmi í Jupyter Notebook

Overview DuckLake lakehouse:
- **Postgres** as the DuckLake catalog
- **MinIO** (S3-compatible) eða **local `./data/`** fyrir Parquet skrár
- **notebooks** loada gögnum og notkun á ducklake með [kaupskrá HMS](https://fasteignaskra.is/gogn/grunngogn-til-nidurhals/kaupskra-fasteigna/)

---

## Quick Start

1. **Configure**
   - Breyta `.env` (velja `DL_STORAGE=s3` or `local`).
   - Breyta um password í MINIO_ROOT_PASSWORD

2. **Keyra upp projectið**
   - clone'a projectið í viðeigandi möppu og hafa terminal í rótinni tilbúið
   - Keyra: `docker compose up --build`
Valkvætt:
   - Hægt er að skoða MinIO UI: `http://localhost:9001` (login með user:pass í `.env`)

3. **Setja upp virtual environment og installa pökkum (fyrir Notebooks)**

   - Uppsetning með UV: `uv sync`
   - Uppsetning með virtulenv: `python -m venv .venv && source .venv/bin/activate` síðan `pip install -e .`
   - *(Windows: `.venv\Scripts\activate`)*

4. **Keyra notebooks**
   - VS Code or Jupyter → run the cells.  

---

## Strúktúr

├─ .env
├─ docker-compose.yml
├─ data/
├─ notebooks/
│ ├─ ducklake_demo.ipynb
├─ app/
│ ├─ app.py
│ ├─ requirements.txt
│ └─ Dockerfile
└─ src/
└─ ducklake/
├─ init.py
└─ setup.p
pyproject.toml


- **Catalog** með Postgres.
- **Data files** MinIO (`s3://<bucket>/<prefix>/…`) eða local `./data/`

---

## Notebooks

- `notebooks/ducklake_demo.ipynb` — Nær í **HMS Kaupskrá** CSV og býr til töflur; fer fyrir **time-travel** og **transactions**.

**HMS data URL**  
`https://frs3o1zldvgn.objectstorage.eu-frankfurt-1.oci.customer-oci.com/n/frs3o1zldvgn/b/public_data_for_download/o/kaupskra.csv`

---

## Config

Edita `.env`:

- Storage: `DL_STORAGE=s3` (MinIO) or `DL_STORAGE=local` # Mæli með s3
- Local path: `DL_DATA_PATH=data/`
- MinIO:
  - `DL_S3_BUCKET`, `DL_S3_PREFIX`, `DL_S3_REGION`
  - `DL_S3_ENDPOINT` (container side), `DL_S3_HOST_ENDPOINT` (notebook side)
  - `S3_URL_STYLE=path`, `S3_USE_SSL=false`
- Postgres: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_PORT`
- Lake alias: `DUCKLAKE_NAME`
- App-Postgres DSN: `DL_PG_CONN`


---

## References

- DuckLake — https://ducklake.select/
- DuckDB — https://duckdb.org/  
  Python API — https://duckdb.org/docs/api/python/overview  
  httpfs — https://duckdb.org/docs/extensions/httpfs

- MinIO — https://min.io/docs/

- PostgreSQL — https://www.postgresql.org/docs/

- Docker Compose — https://docs.docker.com/compose/

---