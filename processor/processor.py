# processor.py: consume from raw queues, clean & enrich, publish to proc queues
#!/usr/bin/env python3

from cleaning_utils import load_csv, clean_df
import typer
import pika
import os
from dotenv import load_dotenv

app = typer.Typer(help="Consume a filepath, then forward it to 'file.proc'")

def get_channel() -> pika.BlockingConnection.channel:
    load_dotenv()  # reads .env in working dir
    creds = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "guest"),
        os.getenv("RABBITMQ_PASS", "guest")
    )
    params = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
        port=int(os.getenv("RABBITMQ_PORT", 5672)),
        credentials=creds
    )
    conn = pika.BlockingConnection(params)
    return conn.channel()

@app.command("process-file")
def process_file():
    """
    1. Pull one message from 'file.raw'
    2. Acknowledge it
    3. Publish the same filepath to 'file.proc'
    """
    ch = get_channel()

    # Ensure queues exist
    ch.queue_declare(queue="file.raw", durable=True)
    ch.queue_declare(queue="file.proc", durable=True)

    # Try to get one message
    method, props, body = ch.basic_get(queue="file.raw", auto_ack=False)
    if method is None:
        typer.secho("⚠️  'file.raw' is empty", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    filepath = body.decode()
    typer.secho(f"📥 Got file path: {filepath}", fg=typer.colors.GREEN)

    # Acknowledge removal from raw queue
    ch.basic_ack(delivery_tag=method.delivery_tag)

    try:
        df = load_csv(filepath)
        df_clean = clean_df(df)
        typer.secho("✅ File loaded and cleaned successfully.", fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"❌ Error processing file: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    # Forward to processed queue
    ch.basic_publish(
        exchange="",
        routing_key="file.proc",
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),
    )
    typer.secho(f" Forwarded to 'file.proc'", fg=typer.colors.BLUE)

if __name__ == "__main__":
    app()
