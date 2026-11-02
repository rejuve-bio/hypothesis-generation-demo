# Shared OpenTargets database

One Postgres database holds the OpenTargets data used by both the Python
pipeline (`src/db/credible_sets_handler.py`) and the Prolog backend
(`pl/queries.pl`). Neither application loads data; they only connect to it.
Everything that sets the database up lives in this folder.

| Path | Purpose |
|---|---|
| `Dockerfile.postgres` | Postgres 15 with the [pg_parquet](https://github.com/CrunchyData/pg_parquet) extension (built from source) |
| `Dockerfile.bootstrap` | Image that runs `ot_db_setup.py` (Python + `pg_dump`/`pg_restore`) |
| `sql/01_raw.sql` | Landing tables for `COPY … FROM '<parquet>'` (OpenTargets field names) |
| `sql/02_tables.sql` | Final tables, named the way the Prolog backend queries them |
| `sql/03_transform.sql` | Raw → final, plus the derived `credible_set_variants` and `gwas_study_index` |
| `sql/04_indexes.sql` | Primary keys and indexes |
| `ot_db_setup.py` | `ensure` / `publish` / `list` |

## How a deployment gets its data
The `ot-db-bootstrap` service runs `ot_db_setup.py ensure` on every deploy, and
the pipeline workers start only after it succeeds:

1. If `ot_load_state` records the pinned release and schema version, and the
   live row counts match the recorded ones, it exits immediately.
2. Otherwise it restores a `pg_dump` snapshot from MinIO
   (`<prefix>/<release>/schema-v<N>/{manifest.json, ot.dump}`), checking its
   sha256 and row counts.
3. If there is no snapshot, it loads the release from the EBI FTP with
   pg_parquet, one `COPY` per parquet file, then runs the SQL transforms. This
   takes hours, so publish a snapshot afterwards.

Loads are built in an `ot_staging` schema and swapped into `public` in one
transaction, so a failed or partial load never replaces good data.

## Configuration
| Variable | Meaning |
|---|---|
| `OT_RELEASE` | Required. Pinned OpenTargets release, e.g. `25.09` (`latest` is rejected). |
| `OT_DB_BOOTSTRAP` | `auto` (default), `minio-only`, `ebi-only`, or `skip` (CI). |
| `OT_DB_SNAPSHOT_MINIO_PREFIX` | Snapshot prefix in the MinIO bucket (default `pg-releases/opentargets`). |
| `CREDIBLE_SETS_DB_*` | Target database. The user must be a superuser (pg_parquet reads https URLs). |
| `MINIO_*` | Snapshot store. |

## Operations
```sh
# publish the loaded release so other deployments restore it from MinIO
docker compose run --rm ot-db-bootstrap publish        # --force to overwrite
docker compose run --rm ot-db-bootstrap list

# force a reload
docker compose exec credible-sets-postgres psql -U "$CREDIBLE_SETS_DB_USER" -d "$CREDIBLE_SETS_DB_NAME" \
  -c "DELETE FROM ot_load_state"
docker compose up ot-db-bootstrap
```

## Changing the schema
Edit `sql/02_tables.sql` (and `01`/`03`/`04` as needed), then bump
`SCHEMA_VERSION` in `ot_db_setup.py`. Every deployment then reloads once.
`db/tests/test_ot_db_setup.py` checks that every column the Prolog and Python
backends query still exists.
