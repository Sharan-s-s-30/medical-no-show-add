#!/usr/bin/env python3
# producer.py: read a CSV, compress & encode it, and broadcast to all downstream consumers via a fanout exchange

from pathlib import Path
import json
import gzip
import base64
import pika
import typer
from dotenv import load_dotenv
import os

app = typer.Typer(help="Publish a compressed CSV batch to RabbitMQ for both raw ingestion and processing")

def get_channel() -> pika.BlockingConnection.channel:
    """Connect to RabbitMQ and return a channel."""
    load_dotenv()  # reads .env in working dir
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

@app.command()
def produce(
    file: Path = typer.Option(..., exists=True, help="Path to your raw CSV file")
):
    """
    1. Read the entire CSV into memory
    2. Compress with gzip and Base64-encode
    3. Broadcast the envelope JSON to the 'raw_data' fanout exchange
    """
    ch = get_channel()

    # 1. Declare a fanout exchange for raw data
    ch.exchange_declare(exchange="raw_data", exchange_type="fanout", durable=True)

    # 2. Read & compress the CSV
    raw_bytes = file.read_bytes()
    comp_bytes = gzip.compress(raw_bytes)
    b64_str = base64.b64encode(comp_bytes).decode("utf-8")

    # 3. Build the JSON envelope
    payload = {
        "type": "compressed_csv",
        "filename": file.name,
        "data_b64": b64_str
    }
    body = json.dumps(payload)

    # 4. Publish to the exchange (fanout => all bound queues get it)
    ch.basic_publish(
        exchange="raw_data",
        routing_key="",  # ignored by fanout
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),
    )
    typer.echo(f"📤 Broadcast compressed CSV '{file.name}' to exchange 'raw_data'")

if __name__ == "__main__":
    app()
