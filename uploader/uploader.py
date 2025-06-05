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

# crosscheck naming
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

    ch = get_channel()

    method, props, body = ch.basic_get(queue="file.raw_ingest", auto_ack=False)
    if method is None:
        typer.secho("'file.raw_ingest' is empty")
        raise typer.Exit(code=1)

    # parse envelope
    msg = json.loads(body)
    data_b64 = msg["data_b64"]
    filename = msg.get("filename", "<unknown>")
    typer.secho(f"Raw ingest received: {filename}")

    # decode & decompress
    comp = base64.b64decode(data_b64)
    csv_bytes = gzip.decompress(comp)

    # load
    df = pd.read_csv(BytesIO(csv_bytes))
    typer.secho(f"Loaded {len(df)} raw rows")

    # normalize and verify
    df = normalize_and_rename(df)

    df = df[[
      "patient_id","appointment_id","gender",
      "scheduled_day","appointment_day","age",
      "neighbourhood","scholarship","hypertension",
      "diabetes","alcoholism","handicap","sms_received","no_show"
    ]]

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
    typer.secho(f"Inserting {len(rows)} raw rows…")
    insert_rows("raw_appointments", rows)

    # ack & exit
    ch.basic_ack(delivery_tag=method.delivery_tag)
    typer.secho(" Raw data inserted; exiting.")

@app.command("upload")
def upload_processed():

    ch = get_channel()

    method, props, body = ch.basic_get(queue="file.proc", auto_ack=False)
    if method is None:
        typer.secho("'file.proc' is empty")
        raise typer.Exit(code=1)

    # parse envelope
    msg = json.loads(body)
    data_b64 = msg["data_b64"]
    filename = msg.get("filename", "<unknown>")
    typer.secho(f"Processed ingest received: {filename}")

    # decode & decompress
    comp = base64.b64decode(data_b64)
    csv_bytes = gzip.decompress(comp)

    # load
    df = pd.read_csv(BytesIO(csv_bytes))
    typer.secho(f"Loaded {len(df)} cleaned rows")

    # bulk insert
    rows = df.to_dict("records")
    typer.secho(f"Inserting {len(rows)} processed rows…")
    insert_rows("processed_appointments", rows)

    # ack & exit
    ch.basic_ack(delivery_tag=method.delivery_tag)
    typer.secho("Processed rows inserted.")

if __name__ == "__main__":
    app()
