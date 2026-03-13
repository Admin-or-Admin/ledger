import sys
import os

from shared.logger import setup_logger
from ledger.config import Config
from ledger.database import Database
from ledger.handlers import Handlers
from shared.kafka_client import AuroraConsumer, AuroraProducer

# Initialize standardized logger
logger = setup_logger("ledger")

def main():
    logger.info("Aurora Normalized Ledger Service starting")
    
    # Initialize Database
    db = Database()
    if Config.DROP_DB:
        db.drop_schema()
    db.init_schema()
    
    # Initialize Kafka Consumer
    group_id = Config.KAFKA_GROUP_ID
    topics = [t for t in Config.TOPICS if t not in Config.EXCLUDE_TOPICS]
    
    if Config.SCRAPE_FROM_BEGINNING:
        import uuid
        group_id = f"{Config.KAFKA_GROUP_ID}-scrape-{uuid.uuid4().hex[:8]}"
        logger.info(f"SCRAPE_FROM_BEGINNING enabled. Using unique group_id: {group_id}", extra={"new_group_id": group_id})
        logger.info(f"Scraping from the beginning of topics: {topics}")
        if Config.EXCLUDE_TOPICS:
            logger.info(f"Excluded topics: {Config.EXCLUDE_TOPICS}")

    consumer = AuroraConsumer(
        topics=topics,
        group_id=group_id,
        bootstrap_servers=Config.KAFKA_BROKERS,
        auto_offset='earliest'
    )

    # Initialize Kafka Producer for sending responses to actions
    producer = AuroraProducer(
        bootstrap_servers=Config.KAFKA_BROKERS
    )

    topic_routing = {
        "logs.unfiltered": Handlers.handle_unfiltered_logs,
        "events.correlated": Handlers.handle_correlated_events,
        "logs.categories": Handlers.handle_categories,
        "logs.solver_plan": Handlers.handle_solver_plan,
        "logs.solution": Handlers.handle_solution,
        "actions": Handlers.handle_actions,
        "analytics": Handlers.handle_analytics
    }

    logger.info(f"Subscribed to topics: {topics}")

    message_count = 0
    topic_counts = {t: 0 for t in topics}

    try:
        for message in consumer:
            topic = message.topic
            handler = topic_routing.get(topic)
            
            if not handler:
                continue

            # Process with transaction management
            try:
                with db.get_cursor() as cur:
                    if topic == "actions":
                        handler(cur, message, producer)
                    else:
                        handler(cur, message)
                    db.commit()
                
                message_count += 1
                topic_counts[topic] += 1
                if message_count % 10 == 0:
                    stats_str = ", ".join([f"{t}: {c}" for t, c in topic_counts.items() if c > 0])
                    logger.info("Batch processing progress", extra={
                        "total_processed": message_count,
                        "counts": topic_counts
                    })
                    
            except Exception as e:
                logger.error(f"Error processing {topic}: {e}", exc_info=True, extra={"topic": topic})
                db.rollback()

    except KeyboardInterrupt:
        logger.info("Stopping Ledger service...")
    finally:
        consumer.close()
        producer.close()
        db.close()

if __name__ == "__main__":
    main()
