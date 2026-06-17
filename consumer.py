import json
import boto3
import pandas as pd
from datetime import datetime, timezone
from kafka import KafkaConsumer
from databricks import sql
import os
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("AWS_S3_BUCKET")

KAFKA_TOPIC = 'transaction_events'
KAFKA_BOOTSTRAP = 'localhost:29092'
BATCH_SIZE = 10

# ============================================================
# CONNECTIONS
# ============================================================

# ---- Kafka consumer ----
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='ledger-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
print("Connected to Kafka — listening for events...")

# ---- S3 client ----
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)
print("Connected to AWS S3...")


# ============================================================
# DATABRICKS
# ============================================================

def get_db_connection():
    """Create and return a Databricks SQL connection."""
    return sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )


def create_table_if_not_exists(cursor):
    """Create the Bronze table if it doesn't already exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS `ledger-catalog`.raw.transactions_raw (
            transaction_id  STRING,
            user_id         STRING,
            user_name       STRING,
            account_number  STRING,
            bank            STRING,
            type            STRING,
            amount          DOUBLE,
            currency        STRING,
            event_timestamp STRING,
            channel         STRING,
            description     STRING,
            ingested_at     STRING,
            raw_payload     STRING
        )
        USING DELTA
    """)
    print("Table ready: ledger-catalog.raw.transactions_raw")


def write_to_databricks(events: list):
    """Write a batch of raw events to the Bronze Delta table."""
    rows = []
    for event in events:
        rows.append({
            'transaction_id': event.get('transaction_id'),
            'user_id':        event.get('user_id'),
            'user_name':      event.get('user_name'),
            'account_number': event.get('account_number'),
            'bank':           event.get('bank'),
            'type':           event.get('type'),
            'amount':         event.get('amount'),
            'currency':       event.get('currency'),
            'event_timestamp': event.get('timestamp'),
            'channel':        event.get('channel'),
            'description':    event.get('description'),
            'ingested_at':    datetime.now(timezone.utc).isoformat(),
            'raw_payload':    json.dumps(event)
        })

    df = pd.DataFrame(rows)

    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            create_table_if_not_exists(cursor)

            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO `ledger-catalog`.raw.transactions_raw
                    (
                        transaction_id,
                        user_id,
                        user_name,
                        account_number,
                        bank,
                        type,
                        amount,
                        currency,
                        event_timestamp,
                        channel,
                        description,
                        ingested_at,
                        raw_payload
                    )
                    VALUES (
                        :transaction_id,
                        :user_id,
                        :user_name,
                        :account_number,
                        :bank,
                        :type,
                        :amount,
                        :currency,
                        :event_timestamp,
                        :channel,
                        :description,
                        :ingested_at,
                        :raw_payload
                    )
                """, row.to_dict())

    print(f"Wrote {len(events)} events to ledger-catalog.raw.transactions_raw")


# ============================================================
# S3
# ============================================================

def is_bad_event(event):
    """Check if an event is malformed."""
    if not event.get('transaction_id'):
        return True, 'missing_transaction_id'
    if not event.get('user_id'):
        return True, 'missing_user_id'
    if not event.get('amount'):
        return True, 'missing_amount'
    if event.get('amount', 0) <= 0:
        return True, 'invalid_amount'
    if event.get('type') not in ['deposit', 'withdrawal', 'transfer']:
        return True, 'invalid_type'
    return False, None


def archive_to_s3(event, error_reason):
    """Dump bad event to S3 as a JSON file."""
    payload = {
        'raw_event': event,
        'error_reason': error_reason,
        'failed_at': datetime.now(timezone.utc).isoformat()
    }

    file_key = (
        f"dlq/"
        f"{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/"
        f"{event.get('transaction_id', 'unknown')}_"
        f"{datetime.now(timezone.utc).strftime('%H%M%S')}.json"
    )

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=file_key,
        Body=json.dumps(payload),
        ContentType='application/json'
    )

    print(f"Archived bad event to S3: {file_key}")


# ============================================================
# MAIN LOOP
# ============================================================

def run():
    """
    Reads events from Kafka topic.
    - Bad events → archived to S3 immediately
    - All events → batched and written to Databricks Bronze table
    """
    batch = []

    for message in consumer:
        event = message.value

        # Check if bad — archive to S3 immediately
        bad, reason = is_bad_event(event)
        if bad:
            print(f"BAD EVENT detected — reason: {reason}")
            archive_to_s3(event, reason)

        # All events go to Bronze regardless
        batch.append(event)

        print(f"📨 Received: {event.get('transaction_id')} | "
              f"user: {event.get('user_name', 'unknown')} | "
              f"type: {event.get('type')} | "
              f"amount: {event.get('amount', 'MISSING')}")

        # Write batch to Databricks every 10 events
        if len(batch) >= BATCH_SIZE:
            print(f"Batch of {BATCH_SIZE} ready — writing to Databricks...")
            write_to_databricks(batch)
            batch = []


if __name__ == '__main__':
    run()