#!/usr/bin/env python3
# uploader.py: consume both raw and cleaned CSV envelopes, decode, and bulk-insert into Postgres

import json
import gzip
import base64
from io import BytesIO

import pandas as pd
import typer
import pika
import os
from dotenv import load_dotenv
from uploader.db_utils import insert_rows


app = typer.Typer(help="Upload raw and processed CSVs from RabbitMQ into Postgres")

def get_channel() -> pika.BlockingConnection.channel:
    load_dotenv()
    creds = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "guest"),
        os.getenv("RABBITMQ_PASS", "guest"),
    )
    params = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
        port=int(os.getenv("RABBITMQ_PORT", 5672)),
        credentials=creds,
    )
    return pika.BlockingConnection(params).channel()

# inside uploader.py, above upload_raw():
def normalize_and_rename(df):
    df = df.copy()
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r"[ \-]", "_", regex=True)
    )
    return df.rename(columns={
        "patientid":       "patient_id",
        "appointmentid":   "appointment_id",
        "scheduledday":    "scheduled_day",
        "appointmentday":  "appointment_day",
        "hipertension":    "hypertension",
        "handcap":         "handicap",
        "no-show":         "no_show",
        "sms_received":    "sms_received",
    })

@app.command("upload-raw")
def upload_raw():
    """
    1. Bind to 'raw_data' fanout → queue 'file.raw_ingest'
    2. Pull one compressed CSV envelope
    3. Decode & decompress
    4. Load into DataFrame
    5. Bulk-insert into raw_appointments
    6. Acknowledge and exit
    """
    ch = get_channel()

    method, props, body = ch.basic_get(queue="file.raw_ingest", auto_ack=False)
    if method is None:
        typer.secho("⚠️  'file.raw_ingest' is empty", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    # parse envelope
    msg = json.loads(body)
    data_b64 = msg["data_b64"]
    filename = msg.get("filename", "<unknown>")
    typer.secho(f"📥 Raw ingest received: {filename}", fg=typer.colors.GREEN)

    # decode & decompress
    comp = base64.b64decode(data_b64)
    csv_bytes = gzip.decompress(comp)

    # load
    df = pd.read_csv(BytesIO(csv_bytes))
    typer.secho(f"✅ Loaded {len(df)} raw rows", fg=typer.colors.GREEN)

    # --- NORMALIZE COLUMNS FOR DB ---
    df = normalize_and_rename(df)

    # ensure only the columns your table expects:
    df = df[[
      "patient_id","appointment_id","gender",
      "scheduled_day","appointment_day","age",
      "neighbourhood","scholarship","hypertension",
      "diabetes","alcoholism","handicap","sms_received","no_show"
    ]]
    # ---------------------------------

    # Normalize column names
    df.columns = (
      df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[ \-]", "_", regex=True)
    )

    # Convert integer flags to bool
    for c in [
      "scholarship","hypertension","diabetes",
      "alcoholism","sms_received","no_show"
    ]:
        df[c] = df[c].astype(bool)

    # bulk insert
    rows = df.to_dict("records")
    typer.secho(f"Inserting {len(rows)} raw rows…", fg=typer.colors.YELLOW)
    insert_rows("raw_appointments", rows)

    # ack & exit
    ch.basic_ack(delivery_tag=method.delivery_tag)
    typer.secho(" Raw data inserted; exiting.", fg=typer.colors.BLUE)

@app.command("upload")
def upload_processed():
    """
    1. Pull one cleaned CSV envelope from 'file.proc'
    2. Decode & decompress
    3. Load into DataFrame
    4. Bulk-insert into processed_appointments
    5. Acknowledge and exit
    """
    ch = get_channel()

    method, props, body = ch.basic_get(queue="file.proc", auto_ack=False)
    if method is None:
        typer.secho("'file.proc' is empty", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    # parse envelope
    msg = json.loads(body)
    data_b64 = msg["data_b64"]
    filename = msg.get("filename", "<unknown>")
    typer.secho(f"Processed ingest received: {filename}", fg=typer.colors.GREEN)

    # decode & decompress
    comp = base64.b64decode(data_b64)
    csv_bytes = gzip.decompress(comp)

    # load
    df = pd.read_csv(BytesIO(csv_bytes))
    typer.secho(f"Loaded {len(df)} cleaned rows", fg=typer.colors.GREEN)

    # bulk insert
    rows = df.to_dict("records")
    typer.secho(f"Inserting {len(rows)} processed rows…", fg=typer.colors.YELLOW)
    insert_rows("processed_appointments", rows)

    # ack & exit
    ch.basic_ack(delivery_tag=method.delivery_tag)
    typer.secho("Processed rows inserted.", fg=typer.colors.BLUE)

if __name__ == "__main__":
    app()
