#!/usr/bin/env python3
# processor.py: consume compressed CSV from 'raw_data' exchange via queue 'file.raw',
# clean it, re-compress & re-publish to 'file.proc'

import json
import gzip
import base64
import typer
import pika
import os
from dotenv import load_dotenv
from cleaning_utils import load_csv_from_bytes, clean_df

app = typer.Typer(help="Consume a  CSV from 'raw_data', clean it, and forward to 'file.proc'")

def get_channel() -> pika.BlockingConnection.channel:
    """Establishes and returns a RabbitMQ channel using environment variables."""
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
    conn = pika.BlockingConnection(params)
    return conn.channel()

@app.command("process-file")
def process_file():
    """Processes one raw CSV message from RabbitMQ and forwards the cleaned result."""

    ch = get_channel()

    # pull one message
    method, props, body = ch.basic_get(queue="file.raw", auto_ack=False)
    if method is None:
        typer.secho("'file.raw' is empty")
        raise typer.Exit(code=1)

    # parse and validate the incoming message
    try:
        msg = json.loads(body)
        if msg.get("type") != "compressed_csv":
            raise ValueError(f"Unexpected message type: {msg.get('type')}")
    except Exception as e:
        typer.secho(f"Invalid message format: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        raise typer.Exit(code=1)

    filename = msg["filename"]
    data_b64 = msg["data_b64"]
    typer.secho(f"Received compressed CSV: {filename}")

    # decode and decompress
    try:
        comp_bytes = base64.b64decode(data_b64)
        raw_bytes = gzip.decompress(comp_bytes)
    except Exception as e:
        typer.secho(f"Error decoding/decompressing: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        raise typer.Exit(code=1)

    # load CSV into DataFrame and clean it
    try:
        df_clean = clean_df(load_csv_from_bytes(raw_bytes))
        typer.secho(f"Cleaned {len(df_clean)} rows")
    except Exception as e:
        typer.secho(f"Error while cleaning CSV: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        raise typer.Exit(code=1)

    # compress and base64-encode the cleaned CSV
    try:
        cleaned_csv_bytes = df_clean.to_csv(index=False).encode("utf-8")
        comp_cleaned = gzip.compress(cleaned_csv_bytes)
        b64_cleaned = base64.b64encode(comp_cleaned).decode("utf-8")
    except Exception as e:
        typer.secho(f"Error while compressing cleaned CSV: {e}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        raise typer.Exit(code=1)

    new_msg = {
        "type":      "compressed_cleaned_csv",
        "filename":  filename,
        "data_b64":  b64_cleaned
    }
    new_body = json.dumps(new_msg)

    # ack and forward
    ch.basic_ack(delivery_tag=method.delivery_tag)
    ch.basic_publish(
        exchange="",
        routing_key="file.proc",
        body=new_body,
        properties=pika.BasicProperties(delivery_mode=2),
    )
    typer.secho("Forwarded cleaned CSV to 'file.proc'")

if __name__ == "__main__":
    app()
