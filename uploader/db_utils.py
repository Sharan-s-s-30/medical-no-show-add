# uploader/db_utils.py

import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor, execute_values
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    """
    Establish and return a new Postgres connection using env vars.
    """
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASS")
    )


def ping() -> bool:
    """
    Return True if we can connect and run a simple SELECT 1.
    """
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        conn.close()
        return True
    except Exception:
        return False


# def insert_row(table: str, data: dict) -> None:
#     """
#     Insert a single row (given by a dict) into the specified table.
#     Uses psycopg2.sql to safely interpolate the table and columns.
#     """
#     conn = get_conn()
#     try:
#         with conn.cursor() as cur:
#             cols = data.keys()
#             values = [data[col] for col in cols]
#             insert = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({placeholders})").format(
#                 table=sql.Identifier(table),
#                 fields=sql.SQL(", ").join(map(sql.Identifier, cols)),
#                 placeholders=sql.SQL(", ").join(sql.Placeholder() * len(cols))
#             )
#             cur.execute(insert, values)
#         conn.commit()
#     finally:
#         conn.close()

def insert_rows(table: str, rows: list[dict]) -> None:
    """
    Bulk‐insert a list of dicts into `table` in a single query.
    Uses psycopg2.extras.execute_values for maximum speed.
    """
    if not rows:
        return

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cols = list(rows[0].keys())
            # build the base INSERT statement
            stmt = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"
            # gather the tuples of values
            values = [[row[col] for col in cols] for row in rows]
            # execute_values does a single INSERT with all rows
            execute_values(cur, stmt, values)
        conn.commit()
    finally:
        conn.close()

def fetch_table(table: str) -> list:
    """
    Fetch all rows from the given table as a list of dicts.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql.SQL("SELECT * FROM {table}").format(
                table=sql.Identifier(table)
            ))
            return cur.fetchall()
    finally:
        conn.close()
