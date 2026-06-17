from __future__ import annotations

import shutil
import sqlite3
from itertools import groupby
from pathlib import Path

import yaml


def _soakdb_path(visit_dir: Path) -> Path:
    return visit_dir / "processing/database" / "soakDBDataFile.sqlite"


def _read_protein(db_path: Path) -> str | None:
    """Return the Protein (target acronym) recorded in a soakDB database."""
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
    try:
        row = con.execute("SELECT Protein FROM soakDB").fetchone()
    finally:
        con.close()
    return row[0] if row else None


def _has_crystal(db_path: Path, container_code, location, dtag) -> bool:
    """True if mainTable holds a row for this puck/position/crystal."""
    query = (
        "SELECT 1 FROM mainTable WHERE Puck = ? AND PuckPosition = ? "
        "AND CrystalName = ? LIMIT 1"
    )
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
    try:
        return (
            con.execute(query, (container_code, location, dtag)).fetchone() is not None
        )
    finally:
        con.close()


def find_xchem_visit_dir(
    xchem_dir: Path, acronym, container_code, location, dtag, log
) -> Path | None:
    """Locate the labxchem visit directory under `xchem_dir` whose target
    matches `acronym`.

    Prefers cached `.user.yaml` files (disambiguating by the crystal's
    puck/position when several visits share a target); falls back to reading
    the `Protein` field from each visit's soakDB database, caching the result
    to `.user.yaml` for next time. Returns None if no visit matches."""
    # tier 1: match via cached .user.yaml
    candidates = []
    for subdir in xchem_dir.iterdir():
        user_yaml = subdir / ".user.yaml"
        if not user_yaml.is_file():
            continue
        with open(user_yaml) as f:
            expt_yaml = yaml.load(f, Loader=yaml.SafeLoader)
        if expt_yaml["data"]["acronym"] == acronym:
            candidates.append(subdir)
            log.info(f"Found user yaml for dtag {dtag} at {user_yaml}")

    if len(candidates) == 1:
        return candidates[0]

    # several visits share this target: disambiguate by the crystal's location
    for visit_dir in candidates:
        db_path = _soakdb_path(visit_dir)
        if not db_path.is_file():
            log.info(f"No .sqlite database at {db_path} for dtag {dtag}, skipping")
            continue
        try:
            if _has_crystal(db_path, container_code, location, dtag):
                log.info(f"labxchem visit {visit_dir} found for dtag {dtag}")
                return visit_dir
        except Exception as e:
            log.info(
                f"Exception whilst reading ligand information from {db_path} "
                f"for dtag {dtag}: {e}"
            )

    # tier 2: no cached match — read Protein from each soakDB, caching as we go
    log.info(f"No matching user yaml in {xchem_dir}, reading soakDB databases...")
    match_dir = None
    for subdir in xchem_dir.iterdir():
        if (subdir / ".user.yaml").exists():
            continue
        db_path = _soakdb_path(subdir)
        if not db_path.is_file():
            continue
        try:
            name = _read_protein(db_path)
        except Exception as e:
            log.info(f"Problem reading .sqlite database for {subdir}: {e}")
            continue
        if name is not None:
            with open(subdir / ".user.yaml", "w") as f:
                yaml.dump({"data": {"acronym": name}}, f)
        if name == acronym:
            match_dir = subdir
    return match_dir


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

    master_conn = sqlite3.connect(db_master)
    try:
        master_col_defs = {
            row[1]: row[2] for row in master_conn.execute(f"PRAGMA table_info({table})")
        }
    finally:
        master_conn.close()

    copy_conn = sqlite3.connect(db_copy)
    try:
        copy_cols = {row[1] for row in copy_conn.execute(f"PRAGMA table_info({table})")}

        new_cols = set(master_col_defs.keys()) - copy_cols
        for col in new_cols:
            col_type = master_col_defs[col]
            copy_conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")

        copy_conn.commit()
    finally:
        copy_conn.close()


def sync_rows_from_master(master_path, copy_path, table):
    conn = sqlite3.connect(copy_path)
    try:
        conn.execute(f"ATTACH DATABASE '{master_path}' AS master")
        cols = [row[1] for row in conn.execute(f"PRAGMA master.table_info({table})")]
        collist = ", ".join(f'"{c}"' for c in cols)

        # Insert only rows from master that don't already exist in the copy
        conn.execute(f"""
            INSERT OR IGNORE INTO {table} ({collist})
            SELECT {collist} FROM master.{table}
        """)

        conn.commit()
        conn.execute("DETACH DATABASE master")
    finally:
        conn.close()


def updatable_crystals(database_path, overwrite=False) -> set[str]:
    """CrystalNames this run is allowed to write — both the skip-set for
    building db_dicts/exporting files and the gate for the bulk update.

    default: rows not yet given a RefinementOutcome.
    overwrite: every crystal row, including manually-curated ones."""
    if overwrite:
        # where = "CrystalName IS NOT NULL"
        where = "(LastUpdated_by = 'gda2' OR LastUpdated_by IS NULL)"
    else:
        where = (
            "RefinementOutcome IS NULL OR RefinementOutcome = '1 - Analysis Pending'"
        )
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
