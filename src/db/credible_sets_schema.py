"""
Connection settings for the shared OpenTargets Postgres database.

The app only reads this database. It is set up outside the app by db/ (see
db/README.md); the table definitions live in db/sql/02_tables.sql.
"""

import os

# ot_load_state.dataset value written by db/ot_db_setup.py.
DATASET = "opentargets"


def connection_params_from_env() -> dict:
    """psycopg2.connect() kwargs for the shared OpenTargets database."""
    return {
        "host": os.environ.get("CREDIBLE_SETS_DB_HOST", "localhost"),
        "port": int(os.environ.get("CREDIBLE_SETS_DB_PORT", "5411")),
        "dbname": os.environ.get("CREDIBLE_SETS_DB_NAME", "credible_sets"),
        "user": os.environ.get("CREDIBLE_SETS_DB_USER", "credsets"),
        "password": os.environ.get("CREDIBLE_SETS_DB_PASSWORD", ""),
    }
