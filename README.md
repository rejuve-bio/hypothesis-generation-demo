# hypothesis-generation.

## Overview

This project is designed to generate hypotheses for the causal relationship between genetic variants and phenotypes using various bioinformatics tools and machine learning models. The system integrates data from multiple sources, performs enrichment analysis, and uses a large language model (LLM) to summarize and generate hypotheses.

### Prerequisites

- Python 3.8+
- MongoDB
- SWI-Prolog
- Docker and Docker Compose

### Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/rejuve-bio/hypothesis-generation-demo.git
    cd hypothesis-generation
    ```

2. Compile the Prolog files (this step assumes you have access to the Prolog files):
    ```sh
    python compile_kb.py --config-path config/kb.yaml --compile-script pl/compile.pl --path-prefix /mnt/d2_nfs/wondwossen/prolog_out_v2 --hook-script pl/hook.pl
    ```

    To compile the files under `prolog_out_v3`, change the `--path-prefix` argument as follows:
    ```sh
    python compile_kb.py --config-path config/kb.yaml --compile-script pl/compile.pl --path-prefix /mnt/d2_nfs/wondwossen/prolog_out_v3 --hook-script pl/hook.pl
    ```

3. Set up environment variables:
    Create a `.env` file in the root directory and add the following variables:
    ```env
    MONGODB_URI=<your_mongodb_uri>
    DB_NAME=<your_db_name>
    OPENAI_API_KEY=<your_openai_api_key>
    ANTHROPIC_API_KEY=<your_anthropic_api_key>
    HF_TOKEN=<your_huggingface_token>
    JWT_SECRET_KEY=<your_jwt_secret_key>
    ```

4. Run the Docker containers:
    ```sh
    sudo docker-compose up --build
    ```

5. Once the containers are running, the Flask app, Prolog service, and MongoDB service will be accessible on ports 5000, 4242, and 27017, respectively.

### OpenTargets credible-sets database

The pipeline reuses OpenTargets' precomputed credible sets (Postgres service `credible-sets-postgres`) when a study is in OpenTargets, skipping local COJO/SuSiE fine-mapping. On every `docker compose up`, the one-shot `ot-db-bootstrap` service makes sure that database holds a complete copy of the pinned release, and `dask-worker` / `prefect-deployment` start only after it exits 0:

1. If `ot_load_state` records the pinned release + schema version and the live row counts match the counts recorded at load time, it exits in seconds.
2. Otherwise it restores a `pg_dump` snapshot from MinIO (`<prefix>/<release>/schema-v<N>/{manifest.json,ot.dump}`, sha256- and row-count-verified).
3. If there is no snapshot, it loads the release from the EBI FTP (slow — can take hours).

Loads go into an `ot_staging` schema and are swapped into place in one transaction, so a crashed or partial load never replaces good data and is retried on the next run.

| Variable | Meaning |
|---|---|
| `OT_RELEASE` | **Required.** Pinned OpenTargets release, e.g. `25.09` (`latest` is rejected). Must include FinnGen R12 studies. |
| `OT_DB_BOOTSTRAP` | `auto` (default), `minio-only`, `ebi-only`, or `skip` (CI). |
| `OT_DB_SNAPSHOT_MINIO_PREFIX` | Snapshot prefix in the MinIO bucket (default `pg-releases/opentargets`). |
| `OT_DB_SNAPSHOT_CACHE_DIR` | Local download dir for snapshots (default `data/pg_releases`). |

After a server has loaded a new release from EBI, publish it so other deployments restore it from MinIO:

```sh
uv run python scripts/pg_releases.py publish   # --force to overwrite
uv run python scripts/pg_releases.py list
```

To force a reload (e.g. to repair the DB by hand), clear the marker and re-run the bootstrap:

```sh
docker compose exec credible-sets-postgres psql -U $CREDIBLE_SETS_DB_USER -d $CREDIBLE_SETS_DB_NAME -c "DELETE FROM ot_load_state"
docker compose up ot-db-bootstrap
```
