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
    Publish the CSV file path to the 'file.raw' queue.
    """
    ch = get_channel()
    # Declare the queue (idempotent)
    ch.queue_declare(queue="file.raw", durable=True)
    # Body is simply the string path
    body = str(file)
    ch.basic_publish(
        exchange="",
        routing_key="file.raw",
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),  # make message persistent
    )
    typer.echo(f" Published file path '{body}' to queue 'file.raw'")

if __name__ == "__main__":
    app()
