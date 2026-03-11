import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "192.168.1.6:29092").split(",")
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:secret@localhost:5432/cybercontrol")
    KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "ledger-group")
    
    TOPICS = [
        "logs.unfiltered",
        "events.correlated",
        "logs.categories",
        "logs.solver_plan",
        "logs.solution",
        "actions",
        "analytics"
    ]
    
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    SCRAPE_FROM_BEGINNING = os.getenv("SCRAPE_FROM_BEGINNING", "false").lower() == "true"
    EXCLUDE_TOPICS = os.getenv("EXCLUDE_TOPICS", "").split(",") if os.getenv("EXCLUDE_TOPICS") else []
    DROP_DB = os.getenv("DROP_DB", "false").lower() == "true"
