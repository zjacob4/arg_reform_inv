"""DuckDB database operations."""

import duckdb
from datetime import datetime
from pathlib import Path
from typing import List

from ..config.settings import settings
from ..config.series_registry import SeriesSpec


def connect() -> duckdb.DuckDBPyConnection:
    """Create and return a DuckDB connection.
    
    Returns:
        DuckDB connection to the configured database path
    """
    db_path = Path(settings.DUCKDB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def create_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create database schema with dimension and fact tables.
    
    Args:
        conn: DuckDB connection
    """
    # Create dimension table for series metadata
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_series (
            series_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            freq VARCHAR NOT NULL,
            source VARCHAR NOT NULL,
            units VARCHAR NOT NULL
        )
    """)
    
    # Create fact table for time series data
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_series (
            series_id VARCHAR NOT NULL,
            ts TIMESTAMP NOT NULL,
            value DOUBLE NOT NULL,
            PRIMARY KEY (series_id, ts),
            FOREIGN KEY (series_id) REFERENCES dim_series(series_id)
        )
    """)
    
    conn.commit()


def upsert_series_meta(conn: duckdb.DuckDBPyConnection, series_specs: List[SeriesSpec]) -> None:
    """Upsert series metadata into dim_series table.
    
    Args:
        conn: DuckDB connection
        series_specs: List of SeriesSpec objects to upsert
    """
    if not series_specs:
        return
    
    # Prepare data for insertion
    data = [
        (
            spec.code,
            spec.name,
            spec.freq,
            spec.source,
            spec.units,
        )
        for spec in series_specs
    ]
    
    # Use DuckDB's INSERT ON CONFLICT for upsert functionality
    for row in data:
        conn.execute("""
            INSERT INTO dim_series 
            (series_id, name, freq, source, units)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (series_id) 
            DO UPDATE SET 
                name = EXCLUDED.name,
                freq = EXCLUDED.freq,
                source = EXCLUDED.source,
                units = EXCLUDED.units
        """, row)
    
    conn.commit()


def upsert_timeseries(
    conn: duckdb.DuckDBPyConnection,
    series_id: str,
    rows: List[tuple[datetime, float]]
) -> None:
    """Upsert time series data into fact_series table.
    
    Args:
        conn: DuckDB connection
        series_id: Series identifier
        rows: List of (datetime, value) tuples
    """
    if not rows:
        return
    
    # Prepare data for insertion
    data = [(series_id, ts, value) for ts, value in rows]
    
    # Use DuckDB's INSERT ON CONFLICT for upsert functionality
    for row in data:
        conn.execute("""
            INSERT INTO fact_series 
            (series_id, ts, value)
            VALUES (?, ?, ?)
            ON CONFLICT (series_id, ts) 
            DO UPDATE SET value = EXCLUDED.value
        """, row)
    
    conn.commit()

