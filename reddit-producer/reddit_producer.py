# reddit_producer.py

import os
import sys
import json
import time
import logging
from datetime import datetime
from dotenv import load_dotenv  # <-- NEW: load .env file

# Load environment variables from .env automatically
load_dotenv()

# External Libraries
import praw
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# --- Logging Setup ---
def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

LOGGER = setup_logger()

# --- Config from Environment ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "reddit_posts")

REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.environ.get("REDDIT_USER_AGENT")
SUBREDDITS = os.environ.get("SUBREDDITS")
PRAW_RATELIMIT_SECONDS = int(os.environ.get("PRAW_RATELIMIT_SECONDS", 360))

# --- Credential validation ---
if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET and REDDIT_USER_AGENT):
    LOGGER.error("Missing Reddit API credentials. Please set them in the .env file.")
    sys.exit(1)

# --- Kafka Delivery Callback ---
def on_delivery(err, msg):
    if err is not None:
        LOGGER.error(
            f"Kafka Delivery Failed: Topic={msg.topic()}, Key={msg.key()}, Error={err.str()}"
        )
    else:
        LOGGER.debug(
            f"Kafka Message Delivered: Topic={msg.topic()}, Partition={msg.partition()}, Offset={msg.offset()}"
        )

# --- Optional: Create Kafka Topic if missing ---
def ensure_topic_exists():
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    metadata = admin_client.list_topics(timeout=5)

    if KAFKA_TOPIC not in metadata.topics:
        LOGGER.info(f"Creating Kafka topic: {KAFKA_TOPIC}")
        new_topic = NewTopic(KAFKA_TOPIC, num_partitions=3, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()
                LOGGER.info(f"Topic {topic} created successfully.")
            except Exception as e:
                LOGGER.error(f"Failed to create topic {topic}: {e}")

# --- Extract fields from Reddit comment ---
def extract_fields(comment):
    author_name = "[deleted]" if not comment.author else getattr(comment.author, "name", "[unavailable]")

    try:
        return {
            "record_uuid": str(comment.id),
            "timestamp_utc": int(comment.created_utc),
            "source_subreddit": comment.subreddit.display_name,
            "author_id": author_name,
            "content_text": comment.body,
            "score_metrics": {
                "score": comment.score,
                "upvote_ratio": getattr(comment, "upvote_ratio", 1.0),
            },
            "link_id": comment.link_id,
            "parent_id": comment.parent_id,
        }
    except Exception as e:
        LOGGER.error(f"Serialization/Extraction Error for comment {comment.id}: {e}")
        return None

# --- Stream data from Reddit and send to Kafka ---
def stream_data(reddit_instance, kafka_producer):
    LOGGER.info(f"Starting stream on subreddits: {SUBREDDITS} to topic: {KAFKA_TOPIC}")
    stream_iterator = reddit_instance.subreddit(SUBREDDITS).stream.comments(skip_existing=True)

    try:
        for comment in stream_iterator:
            payload_dict = extract_fields(comment)
            if not payload_dict:
                continue

            serialized_json = json.dumps(payload_dict).encode("utf-8")
            partition_key = payload_dict["source_subreddit"].encode("utf-8")

            try:
                kafka_producer.produce(
                    KAFKA_TOPIC,
                    key=partition_key,
                    value=serialized_json,
                    callback=on_delivery,
                )
                kafka_producer.poll(0)

            except BufferError:
                LOGGER.warning("Kafka buffer full. Forcing flush via poll(1).")
                kafka_producer.poll(1)

            except KafkaException as ke:
                LOGGER.error(f"Kafka transient error during produce: {ke}")
                continue

    except praw.exceptions.PRAWException as e:
        LOGGER.error(f"PRAW Stream Exception: {e}")
        raise
    except Exception as e:
        LOGGER.critical(f"Critical unhandled exception in stream: {e}")
        raise
    finally:
        LOGGER.info("Streaming loop finished. Flushing remaining Kafka buffer.")
        kafka_producer.flush(timeout=10)

# --- Main ---
def main():
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
        ratelimit_seconds=PRAW_RATELIMIT_SECONDS,
    )
    LOGGER.info("PRAW connection established successfully.")

    producer_conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "acks": "all",
        "enable.idempotence": "true",
        "linger.ms": 50,
        "compression.codec": "snappy",
        "message.timeout.ms": 300000,
    }

    try:
        producer = Producer(producer_conf)
        LOGGER.info(f"Kafka Producer initialized for brokers: {KAFKA_BROKER}")
    except Exception as e:
        LOGGER.critical(f"Failed to initialize Kafka Producer: {e}")
        sys.exit(1)

    # Ensure topic exists
    ensure_topic_exists()

    retry_delay = 2
    max_retries = 10
    attempts = 0

    while attempts < max_retries:
        try:
            stream_data(reddit, producer)
            break
        except Exception as e:
            attempts += 1
            LOGGER.error(f"Attempt {attempts}/{max_retries} failed: {e}. Retrying in {retry_delay}s.")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 600)

    if attempts >= max_retries:
        LOGGER.critical("Maximum retry attempts reached. Producer terminating.")
        sys.exit(1)

if __name__ == "__main__":
    main()
