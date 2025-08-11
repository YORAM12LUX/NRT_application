#!/usr/bin/env python3

import csv
import hashlib
import json
import logging
import os

from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv


load_dotenv()


KAFKA_TOPIC = 'data-nrt'
DATA_FOLDER = './data'
PROCESSED_TRACK_FILE = 'processed_files.json'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_processed_files():
    """Load list of processed files from tracking file."""

    if not os.path.exists(PROCESSED_TRACK_FILE):
        return []
    with open(PROCESSED_TRACK_FILE, "r") as f:
        return json.load(f).get("processed", [])


def save_processed_files(processed_files):
    """Save list of processed files to tracking file."""

    with open(PROCESSED_TRACK_FILE, "w") as f:
        json.dump({"processed": processed_files}, f, indent=2)


def mark_file_as_processed(filename):
    """Mark file as processed in tracking file."""

    processed = load_processed_files()
    if filename not in processed:
        processed.append(filename)
        save_processed_files(processed)


def generate_unique_key(data, source_file):
    """Generate unique key for Kafka message."""
    content = json.dumps(data, sort_keys=True) + source_file
    return hashlib.md5(content.encode('utf-8')).hexdigest()


def create_producer():
    """Create Kafka producer instance."""

    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS").split(","),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None
    )


def process_csv_file(file_path, producer):
    """Process CSV file and send data to Kafka."""

    logger.info(f"Traitement du fichier CSV: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        count = 0
        
        for row in csv_reader:
            message = {
                'data': row,
                'source_file': os.path.basename(file_path),
                'timestamp': datetime.now().isoformat(),
                'processed_at': datetime.now().isoformat()
            }
            unique_key = generate_unique_key(row, os.path.basename(file_path))
            producer.send(KAFKA_TOPIC, key=unique_key, value=message)
            count += 1
            
            if count % 100 == 0:
                logger.info(f"Processed {count} lines")
                
    logger.info(f"File {file_path} fully covered")


def process_json_file(file_path, producer):
    """Process JSON file and send data to Kafka."""

    logger.info(f"Processing the JSON file: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as file:
        try:
            data = json.load(file)
            
            if isinstance(data, list):
                for item in data:
                    message = {
                        'data': item,
                        'source_file': os.path.basename(file_path),
                        'timestamp': datetime.now().isoformat(),
                        'processed_at': datetime.now().isoformat()
                    }
                    unique_key = generate_unique_key(
                        item, os.path.basename(file_path)
                    )
                    producer.send(KAFKA_TOPIC, key=unique_key, value=message)
            else:
                message = {
                    'data': data,
                    'source_file': os.path.basename(file_path),
                    'timestamp': datetime.now().isoformat(),
                    'processed_at': datetime.now().isoformat()
                }
                unique_key = generate_unique_key(
                    data, os.path.basename(file_path)
                )
                producer.send(KAFKA_TOPIC, key=unique_key, value=message)
                
        except json.JSONDecodeError as e:
            logger.error(f"Error JSON dans {file_path}: {e}")


def process_text_file(file_path, producer):
    """Process text file and send data to Kafka."""

    logger.info(f"Processed of file : {file_path}")
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            if line.strip():
                message = {
                    'data': line.strip(),
                    'source_file': os.path.basename(file_path),
                    'timestamp': datetime.now().isoformat(),
                    'processed_at': datetime.now().isoformat()
                }
                unique_key = generate_unique_key(
                    line.strip(), os.path.basename(file_path)
                )
                producer.send(KAFKA_TOPIC, key=unique_key, value=message)


def main():
    """Main function to process files and send to Kafka."""

    if not os.path.exists(DATA_FOLDER):
        logger.error(f"Directory {DATA_FOLDER} doesn't exist")
        return

    try:
        producer = create_producer()
        logger.info("Kafka producer successfully created")
    except Exception as e:
        logger.error(f"Error while creating the producer : {e}")
        return

    processed_files = load_processed_files()

    for filename in sorted(os.listdir(DATA_FOLDER)):
        file_path = os.path.join(DATA_FOLDER, filename)

        if not os.path.isfile(file_path):
            continue
            
        if filename in processed_files:
            logger.info(f"File already processed (ignored) : {filename}")
            continue

        logger.info(f"File to be processed: {filename}")

        try:
            if filename.lower().endswith('.csv'):
                process_csv_file(file_path, producer)
            elif filename.lower().endswith('.json'):
                process_json_file(file_path, producer)
            elif filename.lower().endswith(('.txt', '.log')):
                process_text_file(file_path, producer)
            else:
                logger.warning(f"Unsupported file type: {filename}")
                continue

            mark_file_as_processed(filename)

        except Exception as e:
            logger.error(f"Error during processing of {filename}: {e}")

    producer.flush()
    producer.close()
    logger.info("Successfully completed")


if __name__ == "__main__":
    main()