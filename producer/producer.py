"""
1) Read data/raw/noshowappointments.csv
2) Split into train/test/val
3) Publish JSON rows to raw.train/test/val queues
"""
#!/usr/bin/env python3
from pathlib import Path
import json
import pika
import typer
from dotenv import load_dotenv
from typing import Optional
import os
import gzip
import base64

app = typer.Typer(help="Publish a CSV filepath to RabbitMQ")

def get_channel() -> pika.BlockingConnection.channel:
    """Connect to RabbitMQ and return a channel."""
    load_dotenv()  # reads .env in working dir
    params = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
        port=int(os.getenv("RABBITMQ_PORT", 5672)),
        credentials=pika.PlainCredentials(
            os.getenv("RABBITMQ_USER", "guest"),
            os.getenv("RABBITMQ_PASS", "guest"),
        )
    )
    conn = pika.BlockingConnection(params)
    return conn.channel()

@app.command()
def produce(
    file: Path = typer.Option(..., exists=True, help="Path to your raw CSV file")
):
    """
    Compress & Base64-encode the entire CSV, then publish as JSON to 'file.raw'.
    """
    ch = get_channel()
    ch.queue_declare(queue="file.raw", durable=True)

    # 1. Read the file bytes
    raw_bytes = file.read_bytes()

    # 2. Compress with gzip
    comp_bytes = gzip.compress(raw_bytes)

    # 3. Base64-encode to get a text-safe payload
    b64_str = base64.b64encode(comp_bytes).decode('utf-8')

    # 4. Build the JSON envelope
    payload = {
        "type": "compressed_csv",
        "filename": file.name,
        "data_b64": b64_str
    }
    body = json.dumps(payload)

    # 5. Publish
    ch.basic_publish(
        exchange="",
        routing_key="file.raw",
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),
    )
    typer.echo(f"📤 Published compressed CSV '{file.name}' to 'file.raw'")


if __name__ == "__main__":
    app()
