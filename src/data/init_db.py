"""CLI script to initialize the database."""

import sys

from .db import connect, create_schema, upsert_series_meta
from ..config.series_registry import REGISTRY


def main():
    """Initialize database schema and load series registry."""
    print("Initializing database...")
    
    # Connect to database
    conn = connect()
    
    try:
        # Create schema
        print("Creating database schema...")
        create_schema(conn)
        
        # Load series registry into dim_series
        print(f"Loading {len(REGISTRY)} series into dim_series...")
        series_specs = list(REGISTRY.values())
        upsert_series_meta(conn, series_specs)
        
        print("Database initialized successfully!")
        
        # Show summary
        result = conn.execute("SELECT COUNT(*) FROM dim_series").fetchone()
        print(f"Loaded {result[0]} series into dim_series table")
        
    except Exception as e:
        print(f"Error initializing database: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

