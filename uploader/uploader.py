# uploader.py: consume from raw.* and proc.* queues, insert into Postgres
#!/usr/bin/env python3
import os
import pika
import typer
from dotenv import load_dotenv

app = typer.Typer(help="Consume processed file paths (stub for DB upload)")

def get_channel() -> pika.BlockingConnection.channel:
    load_dotenv()
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

@app.command("upload")
def upload():
    """
    1. Pull one message from 'file.proc'
    2. Acknowledge it
    3. (Placeholder) Print a success message
    """
    ch = get_channel()
    ch.queue_declare(queue="file.proc", durable=True)

    method, props, body = ch.basic_get(queue="file.proc", auto_ack=False)
    if method is None:
        typer.secho("⚠️  'file.proc' is empty", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    filepath = body.decode()
    typer.secho(f"📥 Uploader received: {filepath}", fg=typer.colors.GREEN)
    ch.basic_ack(delivery_tag=method.delivery_tag)

    # TODO: insert into Postgres
    typer.secho(f"✅ (stub) Processed upload for {filepath}", fg=typer.colors.BLUE)

if __name__ == "__main__":
    app()
