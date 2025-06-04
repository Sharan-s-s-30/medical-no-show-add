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
    file: Path = typer.Option(..., exists=True, help="Path to raw CSV file")
):
    """Compresses a local CSV file and publishes it to the 'raw_data' RabbitMQ exchange."""
    ch = get_channel()


    # read the CSV and compress it with gzip, then Base64-encode
    raw_bytes = file.read_bytes()
    comp_bytes = gzip.compress(raw_bytes)
    b64_str = base64.b64encode(comp_bytes).decode("utf-8")

    # build the message payload
    payload = {
        "type": "compressed_csv",
        "filename": file.name,
        "data_b64": b64_str
    }
    body = json.dumps(payload)

    # publish the message to the exchange (fanout sends to all bound queues)
    ch.basic_publish(
        exchange="raw_data",
        routing_key="",  # not used in fanout
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),
    )
    typer.echo(f"Broadcast compressed CSV '{file.name}' to exchange 'raw_data'")

if __name__ == "__main__":
    app()
