
import asyncio
import asyncpg
import json
import logging
import os

from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_TOPIC = 'data-nrt'
DB_CONFIG = {
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': 'localhost',
    'port': '5432'
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_consumer():
    """Create Kafka consumer instance."""
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS").split(","),
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='data-consumer-group'
    )


async def create_database_connection():
    """Create connection to PostgreSQL database."""
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        await create_table_if_not_exists(conn)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None


async def create_table_if_not_exists(conn):
    """Create table if it does not exist."""
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS data_rs (
                id SERIAL PRIMARY KEY,
                col1 INTEGER,
                col2 INTEGER,           
                source_file TEXT,
                timestamp TIMESTAMPTZ,
                processed_at TIMESTAMPTZ
            )
        """)
        logger.info("kafka_data table checked/created.")
    except Exception as e:
        logger.error(f"Error while creating table: {e}")


async def insert_data(conn, message):
    """Insert data into database."""
    try:
        #ISO8601 timestamps to datetime
        ts = None
        if message.get('timestamp'):
            ts = datetime.fromisoformat(message.get('timestamp'))
            
        pa = None
        if message.get('processed_at'):
            pa = datetime.fromisoformat(message.get('processed_at'))

        insert_query = """
        INSERT INTO data_rs (col1,col2, source_file, timestamp, processed_at)
        VALUES ($1, $2, $3, $4, $5)
        """

        data = message.get('data', {})  
        col1 = int(data.get('col1')) if data.get('col1') is not None else None
        col2 = int(data.get('col2')) if data.get('col2') is not None else None

        await conn.execute(
            insert_query,
            col1,                           
            col2,                           
            message.get('source_file'),     
            ts,                              
            pa                               
        )
        
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion des données: {e}")


async def consume_and_insert():
    """Consume Kafka messages and insert into database."""

    consumer = create_consumer()
    logger.info("Kafka consumer successfully created")
    conn = await create_database_connection()
    if not conn:
        return

    try:
        for message in consumer:
            await insert_data(conn, message.value)
            
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        await conn.close()


def main():
    """Main function to start Kafka consumer."""
    asyncio.run(consume_and_insert())


if __name__ == "__main__":
    main()