# Kafka Lab: CSV Producer to Two Topics
Мінімальний лабораторний проєкт для запуску Kafka-кластера в Docker Compose та надсилання JSON-повідомлень із CSV-файлу у два топіки: `Topic1` і `Topic2`.

У проєкті не використовується Avro та Schema Registry, оскільки для цієї лабораторної роботи вони не є обов’язковими.

## Структура проєкту

```text
.
├── docker-compose.yml
├── producer
│   ├── Dockerfile
│   ├── data.csv
│   ├── producer.py
│   └── requirements.txt
└── README.md
```
Що запускається
zookeeper — Zookeeper для Kafka-кластера.
kafka-broker1 — перший Kafka broker.
kafka-broker2 — другий Kafka broker.
kafka-init — одноразовий контейнер, який створює Topic1 і Topic2.
kafka-ui — вебінтерфейс Kafka UI на localhost:8080.
producer — Python producer, який читає producer/data.csv, формує JSON для кожного рядка та надсилає кожне повідомлення одночасно в Topic1 і Topic2.
