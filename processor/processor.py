#!/usr/bin/env python3
# processor.py: consume compressed CSV from 'file.raw', clean it, re-compress & re-publish to 'file.proc'

import json
import gzip
import base64
from io import BytesIO
import typer
import pika
import os
from dotenv import load_dotenv
from cleaning_utils import load_csv_from_bytes, clean_df

app = typer.Typer(help="Consume a compressed CSV, clean it, and forward cleaned CSV to 'file.proc'")

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
    conn = pika.BlockingConnection(params)
    return conn.channel()

@app.command("process-file")
def process_file():
    """
    1. Pull one message from 'file.raw'
    2. Decode, decompress, load & clean the CSV
    3. Re-encode the cleaned CSV (compress & base64)
    4. Acknowledge the original message
    5. Publish the new envelope to 'file.proc'
    """
    ch = get_channel()
    ch.queue_declare(queue="file.raw", durable=True)
    ch.queue_declare(queue="file.proc", durable=True)

    method, props, body = ch.basic_get(queue="file.raw", auto_ack=False)
    if method is None:
        typer.secho("⚠️  'file.raw' is empty", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    # Parse JSON envelope
    try:
        msg = json.loads(body)
        if msg.get("type") != "compressed_csv":
            raise ValueError(f"Unexpected message type: {msg.get('type')}")
    except Exception as e:
        typer.secho(f"❌ Invalid message format: {e}", fg=typer.colors.RED)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        raise typer.Exit(code=1)

    filename = msg["filename"]
    data_b64  = msg["data_b64"]
    typer.secho(f"📥 Received compressed CSV: {filename}", fg=typer.colors.GREEN)

    # Decode & decompress
    try:
        comp_bytes = base64.b64decode(data_b64)
        raw_bytes  = gzip.decompress(comp_bytes)
    except Exception as e:
        typer.secho(f"❌ Error decoding/decompressing: {e}", fg=typer.colors.RED)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        raise typer.Exit(code=1)

    # Load & clean
    try:
        df_clean = clean_df(load_csv_from_bytes(raw_bytes))
        typer.secho(f"✅ Cleaned {len(df_clean)} rows", fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"❌ Error cleaning CSV: {e}", fg=typer.colors.RED)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        raise typer.Exit(code=1)

    # Convert cleaned DataFrame back to CSV bytes
    try:
        cleaned_csv_bytes = df_clean.to_csv(index=False).encode("utf-8")
        comp_cleaned = gzip.compress(cleaned_csv_bytes)
        b64_cleaned  = base64.b64encode(comp_cleaned).decode("utf-8")
    except Exception as e:
        typer.secho(f"❌ Error compressing cleaned CSV: {e}", fg=typer.colors.RED)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        raise typer.Exit(code=1)

    # Build new JSON envelope
    new_msg = {
        "type": "compressed_cleaned_csv",
        "filename": filename,
        "data_b64": b64_cleaned
    }
    new_body = json.dumps(new_msg)

    # Acknowledge original and publish cleaned envelope
    ch.basic_ack(delivery_tag=method.delivery_tag)
    ch.basic_publish(
        exchange="",
        routing_key="file.proc",
        body=new_body,
        properties=pika.BasicProperties(delivery_mode=2),
    )
    typer.secho("📤 Forwarded cleaned CSV to 'file.proc'", fg=typer.colors.BLUE)

if __name__ == "__main__":
    app()
