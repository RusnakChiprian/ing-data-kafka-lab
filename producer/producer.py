import csv
import json
import os
import sys
import time
from typing import Iterable

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-broker1:29092,kafka-broker2:29093",
)
TOPICS = [
    topic.strip()
    for topic in os.getenv("KAFKA_TOPICS", "Topic1,Topic2").split(",")
    if topic.strip()
]
CSV_FILE = os.getenv("CSV_FILE", "/app/data.csv")
MAX_WAIT_SECONDS = int(os.getenv("KAFKA_MAX_WAIT_SECONDS", "120"))


def wait_for_kafka() -> KafkaProducer:
    deadline = time.time() + MAX_WAIT_SECONDS
    attempt = 1

    while time.time() < deadline:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda value: json.dumps(
                    value,
                    ensure_ascii=False,
                ).encode("utf-8"),
                key_serializer=lambda value: value.encode("utf-8"),
                acks="all",
                retries=5,
            )
            print("Kafka is ready.")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka is not ready yet, retry {attempt}...")
            attempt += 1
            time.sleep(5)

    raise TimeoutError(f"Kafka was not ready after {MAX_WAIT_SECONDS} seconds.")


def read_csv_rows(path: str) -> Iterable[dict]:
    with open(path, newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row_number, row in enumerate(reader, start=1):
            yield {
                "row_number": row_number,
                "student_id": int(row["student_id"]),
                "student_name": row["student_name"],
                "course": row["course"],
                "grade": float(row["grade"]),
                "event_time": row["event_time"],
            }


def main() -> int:
    if not TOPICS:
        print("No Kafka topics configured.", file=sys.stderr)
        return 1

    producer = wait_for_kafka()
    sent_count = 0

    try:
        for message in read_csv_rows(CSV_FILE):
            key = str(message["student_id"])

            for topic in TOPICS:
                producer.send(topic, key=key, value=message)

            sent_count += 1
            print(f"Sent row {message['row_number']} to: {', '.join(TOPICS)}")

        producer.flush()
        print(f"Done. Sent {sent_count} messages to each topic.")
        return 0
    finally:
        producer.close()


if __name__ == "__main__":
    raise SystemExit(main())
