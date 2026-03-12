import time
import logging
import psycopg2
from .config import Config

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_url=Config.DATABASE_URL):
        self.db_url = db_url
        self.conn = self._connect_with_retry()

    def _connect_with_retry(self, retries=5, delay=2):
        """Retries database connection until success."""
        while True:
            try:
                conn = psycopg2.connect(self.db_url)
                logger.info("Connected to database.")
                return conn
            except Exception as e:
                logger.warning(f"Database connection failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)

    def drop_schema(self):
        """Drops all project tables. Use with caution!"""
        logger.warning("DROPPING DATABASE SCHEMA (DROP_DB=true)...")
        tables = [
            "chat_messages",
            "chat_sessions",
            "follow_up_actions",
            "incidents",
            "remediation_steps",
            "threat_assessments",
            "classifications",
            "correlations",
            "logs"
        ]
        try:
            with self.conn.cursor() as cur:
                for table in tables:
                    logger.info(f"  [Schema] Dropping table: {table}")
                    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
                self.conn.commit()
            logger.info("Database schema dropped successfully.")
        except Exception as e:
            logger.error(f"Failed to drop schema: {e}")
            self.conn.rollback()

    def init_schema(self):
        """Ensures the normalized database schema exists and is up to date."""
        logger.info("Initializing database schema...")
        
        try:
            with self.conn.cursor() as cur:
                # 1. Core logs table
                logger.info("  [Schema] Ensuring 'logs' table exists...")
                cur.execute("""
                CREATE TABLE IF NOT EXISTS logs (
                    id VARCHAR PRIMARY KEY,
                    timestamp TIMESTAMPTZ NOT NULL,
                    message TEXT,
                    service_name VARCHAR,
                    raw_severity VARCHAR,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """)

                logger.info("  [Schema] Ensuring 'logs' columns are present...")
                cur.execute("ALTER TABLE logs ADD COLUMN IF NOT EXISTS user_id VARCHAR;")
                cur.execute("ALTER TABLE logs ADD COLUMN IF NOT EXISTS http_status INTEGER;")
                cur.execute("ALTER TABLE logs ADD COLUMN IF NOT EXISTS process_pid INTEGER;")
                cur.execute("ALTER TABLE logs ADD COLUMN IF NOT EXISTS trace_id VARCHAR;")
                cur.execute("ALTER TABLE logs ADD COLUMN IF NOT EXISTS source VARCHAR;")
                cur.execute("ALTER TABLE logs ADD COLUMN IF NOT EXISTS processing_stage VARCHAR DEFAULT 'unfiltered';")

                # 1.5. Correlations
                logger.info("  [Schema] Ensuring 'correlations' table exists...")
                cur.execute("""
                CREATE TABLE IF NOT EXISTS correlations (
                    event_id VARCHAR PRIMARY KEY,
                    rule_id VARCHAR,
                    rule_name VARCHAR,
                    strategy VARCHAR,
                    severity VARCHAR,
                    confidence REAL,
                    timestamp TIMESTAMPTZ,
                    involved_entities VARCHAR[],
                    involved_sources VARCHAR[],
                    event_count INTEGER,
                    mitre_tactics VARCHAR[],
                    mitre_techniques VARCHAR[],
                    attack_stage VARCHAR,
                    description TEXT,
                    recommended_action TEXT,
                    metadata JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """)

                # 2. Classification data
                logger.info("  [Schema] Ensuring 'classifications' table exists...")
                cur.execute("""
                CREATE TABLE IF NOT EXISTS classifications (
                    log_id VARCHAR PRIMARY KEY REFERENCES logs(id) ON DELETE CASCADE,
                    category VARCHAR,
                    severity VARCHAR,
                    is_cybersecurity BOOLEAN,
                    confidence INTEGER,
                    reasoning TEXT,
                    tags VARCHAR[],
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """)

                # 3. Threat Assessment
                logger.info("  [Schema] Ensuring 'threat_assessments' table exists...")
                cur.execute("""
                CREATE TABLE IF NOT EXISTS threat_assessments (
                    log_id VARCHAR PRIMARY KEY REFERENCES logs(id) ON DELETE CASCADE,
                    attack_vector VARCHAR,
                    ai_suggestion TEXT,
                    complexity VARCHAR,
                    recurrence_rate INTEGER,
                    confidence INTEGER,
                    auto_fixable BOOLEAN,
                    requires_approval BOOLEAN,
                    priority INTEGER,
                    notify_teams VARCHAR[],
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """)

                # 4. Remediation Steps
                logger.info("  [Schema] Ensuring 'remediation_steps' table exists...")
                cur.execute("""
                CREATE TABLE IF NOT EXISTS remediation_steps (
                    id SERIAL PRIMARY KEY,
                    log_id VARCHAR REFERENCES logs(id) ON DELETE CASCADE,
                    step_number INTEGER,
                    title VARCHAR,
                    description TEXT,
                    command TEXT,
                    risk VARCHAR,
                    estimated_time VARCHAR,
                    rollback TEXT,
                    auto_execute BOOLEAN,
                    requires_approval BOOLEAN,
                    status VARCHAR DEFAULT 'pending',
                    executed_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """)

                # 5. Incidents
                logger.info("  [Schema] Ensuring 'incidents' table exists...")
                cur.execute("""
                CREATE TABLE IF NOT EXISTS incidents (
                    incident_id VARCHAR PRIMARY KEY,
                    log_id VARCHAR UNIQUE REFERENCES logs(id) ON DELETE CASCADE,
                    resolution_mode VARCHAR,
                    executive_summary TEXT,
                    outcome VARCHAR,
                    resolved_by VARCHAR,
                    what_happened TEXT,
                    impact_assessment TEXT,
                    root_cause TEXT,
                    lessons_learned TEXT,
                    resolved_at TIMESTAMPTZ DEFAULT NOW()
                );
                """)

                # 6. Follow-up Actions
                logger.info("  [Schema] Ensuring 'follow_up_actions' table exists...")
                cur.execute("""
                CREATE TABLE IF NOT EXISTS follow_up_actions (
                    id SERIAL PRIMARY KEY,
                    incident_id VARCHAR REFERENCES incidents(incident_id) ON DELETE CASCADE,
                    title VARCHAR,
                    description TEXT,
                    owner VARCHAR,
                    deadline VARCHAR,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """)

                # 7. Analytics
                logger.info("  [Schema] Ensuring 'analytics' table exists...")
                cur.execute("""
                CREATE TABLE IF NOT EXISTS analytics (
                    id SERIAL PRIMARY KEY,
                    agent VARCHAR,
                    event VARCHAR,
                    timestamp TIMESTAMPTZ,
                    data JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """)

                # 8. Chat Sessions & Messages
                logger.info("  [Schema] Ensuring 'chat_sessions' table exists...")
                cur.execute("""
                CREATE TABLE IF NOT EXISTS chat_sessions (
                    id VARCHAR PRIMARY KEY, -- using UUID generated by app
                    title TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                """)

                logger.info("  [Schema] Ensuring 'chat_messages' table exists...")
                cur.execute("""
                CREATE TABLE IF NOT EXISTS chat_messages (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR REFERENCES chat_sessions(id) ON DELETE CASCADE,
                    role VARCHAR NOT NULL,
                    content TEXT NOT NULL,
                    timestamp TIMESTAMPTZ DEFAULT NOW()
                );
                """)

                self.conn.commit()
            logger.info("Normalized database schema ready.")
        except Exception as e:
            logger.error(f"Failed to initialize schema: {e}")
            self.conn.rollback()

    def get_cursor(self):
        """Helper to get a database cursor."""
        try:
            return self.conn.cursor()
        except psycopg2.InterfaceError:
            self.conn = self._connect_with_retry()
            return self.conn.cursor()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        if self.conn:
            self.conn.close()
