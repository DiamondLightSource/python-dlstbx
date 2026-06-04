from __future__ import annotations

import shutil
import sqlite3
from itertools import groupby
from pathlib import Path


def prepare_auto_db(processing_dir: Path) -> Path:
    """Create (if needed) and sync the auto soakDB copy from the master, then
    return its path. The copy is what updatable_crystals() and the bulk update
    operate on."""
    db_master = processing_dir / "database" / "soakDBDataFile.sqlite"
    db_copy = processing_dir / "auto/database" / "autosoakDBDataFile.sqlite"

    if not db_copy.exists():
        Path(db_copy.parents[0]).mkdir(parents=True, exist_ok=True)
        shutil.copy(db_master, db_copy)

    sync_schema_from_master(db_master, db_copy, "mainTable")
    sync_rows_from_master(db_master, db_copy, "mainTable")
    return db_copy


def sync_schema_from_master(db_master, db_copy, table):
    """Add any columns present in master but missing from copy.
    Preserves column type from master."""

    with sqlite3.connect(db_master) as master_conn:
        master_col_defs = {
            row[1]: row[2] for row in master_conn.execute(f"PRAGMA table_info({table})")
        }

    with sqlite3.connect(db_copy) as copy_conn:
        copy_cols = {row[1] for row in copy_conn.execute(f"PRAGMA table_info({table})")}

        new_cols = set(master_col_defs.keys()) - copy_cols
        for col in new_cols:
            col_type = master_col_defs[col]
            copy_conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")

        copy_conn.commit()


def sync_rows_from_master(master_path, copy_path, table):
    conn = sqlite3.connect(copy_path)
    conn.execute(f"ATTACH DATABASE '{master_path}' AS master")

    # Insert only rows from master that don't already exist in the copy
    conn.execute(f"""
        INSERT OR IGNORE INTO {table}
        SELECT * FROM master.{table}
    """)

    conn.commit()
    conn.execute("DETACH DATABASE master")
    conn.close()


def updatable_crystals(database_path, overwrite=False) -> set[str]:
    """CrystalNames this run is allowed to write — both the skip-set for
    building db_dicts/exporting files and the gate for the bulk update.

    default: rows not yet given a RefinementOutcome.
    overwrite: also rows whose RefinementOutcome was set by a previous
    automated run (LastUpdated_by 'gda2', or never touched), while leaving
    manually-curated rows (any other LastUpdated_by) alone."""
    if overwrite:
        where = "(LastUpdated_by = 'gda2' OR LastUpdated_by IS NULL)"
    else:
        where = "RefinementOutcome IS NULL"
    conn = sqlite3.connect(database_path, timeout=30)
    try:
        rows = conn.execute(
            f"SELECT CrystalName FROM mainTable WHERE {where}"
        ).fetchall()
    finally:
        conn.close()
    return {row[0] for row in rows}


def update_data_source_bulk(db_dicts, database_path):
    # db_dicts is already restricted to updatable_crystals(), so the bulk
    # update only needs to match on CrystalName.
    keyed = sorted(db_dicts, key=lambda d: tuple(sorted(d)))

    conn = sqlite3.connect(database_path, timeout=30)
    try:
        cursor = conn.cursor()
        for keys, group in groupby(keyed, key=lambda d: tuple(sorted(d))):
            columns = [k for k in keys if k != "CrystalName"]
            sql = (
                "UPDATE mainTable SET "
                + ", ".join([f"{col} = :{col}" for col in columns])
                + " WHERE CrystalName = :CrystalName"
            )
            cursor.executemany(sql, list(group))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
