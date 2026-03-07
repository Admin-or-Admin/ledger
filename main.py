import json
import time
import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer

# Configuration (Matches docker-compose and README)
KAFKA_BOOTSTRAP_SERVERS = ["localhost:29092"]
DATABASE_URL = "postgresql://admin:secret@localhost:5432/cybercontrol"
KAFKA_GROUP_ID = "ledger-group"

def get_db_connection():
    """Retries database connection until success."""
    while True:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            return conn
        except Exception as e:
            print(f"Database connection failed ({e}), retrying in 2s...")
            time.sleep(2)

def init_db():
    """Creates the logs table if it doesn't exist."""
    print("Initializing database schema...")
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Using the schema from .github/README.md
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS logs (
        id                  VARCHAR PRIMARY KEY,
        elastic_index       VARCHAR,
        timestamp           TIMESTAMPTZ,
        message             TEXT,
        service             VARCHAR,
        host                VARCHAR,
        environment         VARCHAR,
        raw_severity        VARCHAR,
        category            VARCHAR,
        severity            VARCHAR,
        tags                JSONB,
        is_cybersecurity    BOOLEAN,
        classification_confidence INTEGER,
        classification_reasoning  TEXT,
        ai_suggestion       TEXT,
        attack_vector       TEXT,
        recurrence_rate     INTEGER,
        complexity          VARCHAR,
        auto_fixable        BOOLEAN,
        requires_human_approval BOOLEAN,
        proposed_steps      JSONB,
        correlated_log_ids  JSONB,
        correlation_insight TEXT,
        notify_teams        JSONB,
        resolution_outcome  VARCHAR,
        resolved_by         VARCHAR,
        human_approved_by   VARCHAR,
        step_results        JSONB,
        incident_id         VARCHAR,
        processing_stage    VARCHAR,
        created_at          TIMESTAMPTZ DEFAULT NOW(),
        updated_at          TIMESTAMPTZ DEFAULT NOW()
    );
    """
    cur.execute(create_table_sql)
    conn.commit()
    cur.close()
    conn.close()
    print("Database schema ready.")

# --- HANDLERS ---
# You can easily add more handlers here for new topics

def handle_unfiltered_logs(cur, message_value):
    """Processes messages from 'logs.unfiltered' topic."""
    log_id = message_value.get('trace.id') or message_value.get('id')
    if not log_id:
        return

    print(f"  [Ledger] Persisting unfiltered log: {log_id}")
    
    sql = """
    INSERT INTO logs (id, timestamp, message, service, raw_severity, processing_stage)
    VALUES (%s, %s, %s, %s, %s, 'unfiltered')
    ON CONFLICT (id) DO UPDATE SET
        updated_at = NOW();
    """
    cur.execute(sql, (
        log_id,
        message_value.get('@timestamp'),
        message_value.get('message'),
        message_value.get('service.name'),
        message_value.get('log.level'),
    ))

def handle_categories(cur, message_value):
    """Processes messages from 'logs.categories' topic."""
    # (Future implementation)
    print(f"  [Ledger] Categorization update for log: {message_value.get('id')}")
    pass

# --- ROUTING SYSTEM ---

TOPIC_HANDLERS = {
    "logs.unfiltered": handle_unfiltered_logs,
    "logs.categories": handle_categories,
}

def main():
    init_db()
    
    topics = list(TOPIC_HANDLERS.keys())
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    print(f"Subscribing to topics: {topics}")
    
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        api_version=(3, 4, 0)
    )

    db_conn = get_db_connection()
    
    print("Ledger is active and listening...")
    try:
        for message in consumer:
            topic = message.topic
            handler = TOPIC_HANDLERS.get(topic)
            
            if handler:
                with db_conn.cursor() as cur:
                    try:
                        handler(cur, message.value)
                        db_conn.commit()
                    except Exception as e:
                        print(f"Error processing message from {topic}: {e}")
                        db_conn.rollback()
            else:
                print(f"Warning: No handler for topic {topic}")

    except KeyboardInterrupt:
        print("\nStopping ledger...")
    finally:
        consumer.close()
        db_conn.close()

if __name__ == "__main__":
    main()
