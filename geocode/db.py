import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import config

_SCHEMA = """
CREATE TABLE IF NOT EXISTS highways (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Original fields from CSV
    state TEXT NOT NULL,
    highway_name TEXT NOT NULL,
    route_no TEXT,
    from_location TEXT,
    to_location TEXT,
    county TEXT,
    person_name TEXT,

    -- Phase 1: Normalization
    tier INTEGER,
    normalized_routes TEXT,
    parsed_from TEXT,
    parsed_to TEXT,
    parsed_county TEXT,

    -- Phase 2: Geocoded endpoints
    from_lat REAL,
    from_lon REAL,
    from_geocode_source TEXT,
    to_lat REAL,
    to_lon REAL,
    to_geocode_source TEXT,

    -- Phase 3: Route path
    path_geojson TEXT,
    path_source TEXT,
    path_length_miles REAL,

    -- Phase 4: Centroid + final
    centroid_lat REAL,
    centroid_lon REAL,
    centroid_source TEXT,

    -- Pipeline status
    status TEXT DEFAULT 'pending',
    confidence TEXT,
    error_notes TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_status ON highways(status);
CREATE INDEX IF NOT EXISTS idx_tier ON highways(tier);
CREATE INDEX IF NOT EXISTS idx_state ON highways(state);
"""


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(config.DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = _connect()
    conn.executescript(_SCHEMA)
    conn.commit()
    conn.close()


def import_csv(csv_path: Path | str | None = None):
    """Bulk-insert rows from the input CSV. Skips if data is already imported."""
    csv_path = csv_path or config.INPUT_CSV
    conn = _connect()

    count = conn.execute("SELECT COUNT(*) FROM highways").fetchone()[0]
    if count > 0:
        print(f"  DB already has {count} rows — skipping CSV import.")
        conn.close()
        return count

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            rows.append((
                r.get("state", "").strip(),
                r.get("highway_name", "").strip(),
                r.get("route_no", "").strip() or None,
                r.get("from_location", "").strip() or None,
                r.get("to_location", "").strip() or None,
                r.get("county", "").strip() or None,
                r.get("person_name", "").strip() or None,
            ))

    conn.executemany(
        """INSERT INTO highways
           (state, highway_name, route_no, from_location, to_location, county, person_name)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()
    inserted = len(rows)
    print(f"  Imported {inserted} rows from CSV.")
    conn.close()
    return inserted


def get_rows_by_status(status: str) -> list[dict]:
    conn = _connect()
    rows = conn.execute(
        "SELECT * FROM highways WHERE status = ? ORDER BY id", (status,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_rows_by_tier(tier: int, status: str | None = None) -> list[dict]:
    conn = _connect()
    if status:
        rows = conn.execute(
            "SELECT * FROM highways WHERE tier = ? AND status = ? ORDER BY id",
            (tier, status),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM highways WHERE tier = ? ORDER BY id", (tier,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_rows() -> list[dict]:
    conn = _connect()
    rows = conn.execute("SELECT * FROM highways ORDER BY id").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_row(row_id: int, **fields):
    """Update specific columns on a row and commit immediately."""
    if not fields:
        return
    fields["updated_at"] = datetime.now(timezone.utc).isoformat()
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [row_id]
    conn = _connect()
    conn.execute(f"UPDATE highways SET {set_clause} WHERE id = ?", values)
    conn.commit()
    conn.close()


def append_error(row_id: int, note: str):
    """Append a note to error_notes without clobbering previous notes."""
    conn = _connect()
    existing = conn.execute(
        "SELECT error_notes FROM highways WHERE id = ?", (row_id,)
    ).fetchone()
    prev = existing["error_notes"] or "" if existing else ""
    combined = f"{prev} | {note}".strip(" |") if prev else note
    conn.execute(
        "UPDATE highways SET error_notes = ?, updated_at = ? WHERE id = ?",
        (combined, datetime.now(timezone.utc).isoformat(), row_id),
    )
    conn.commit()
    conn.close()


def get_stats() -> dict:
    """Return counts by status and tier for progress reporting."""
    conn = _connect()
    status_rows = conn.execute(
        "SELECT status, COUNT(*) as cnt FROM highways GROUP BY status ORDER BY status"
    ).fetchall()
    tier_rows = conn.execute(
        "SELECT tier, COUNT(*) as cnt FROM highways GROUP BY tier ORDER BY tier"
    ).fetchall()
    total = conn.execute("SELECT COUNT(*) FROM highways").fetchone()[0]
    conn.close()
    return {
        "total": total,
        "by_status": {r["status"]: r["cnt"] for r in status_rows},
        "by_tier": {r["tier"]: r["cnt"] for r in tier_rows},
    }
