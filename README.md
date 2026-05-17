# Kafka Lab: CSV Producer to Two Topics

Минимальный лабораторный проект для запуска Kafka-кластера в Docker Compose и отправки JSON-сообщений из CSV-файла в два топика: `Topic1` и `Topic2`.

В проекте нет Avro и Schema Registry, потому что для этой лабораторной они не требуются.

## Структура проекта

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

## Что запускается

- `zookeeper` - Zookeeper для Kafka-кластера.
- `kafka-broker1` - первый Kafka broker.
- `kafka-broker2` - второй Kafka broker.
- `kafka-init` - одноразовый контейнер, который создает `Topic1` и `Topic2`.
- `kafka-ui` - веб-интерфейс Kafka UI на `localhost:8080`.
- `producer` - Python producer, который читает `producer/data.csv`, формирует JSON для каждой строки и отправляет каждое сообщение одновременно в `Topic1` и `Topic2`.

## Как запустить проект

Из корня проекта выполните:

```bash
docker compose up --build
```

Producer ожидает готовности Kafka перед отправкой сообщений. После успешной отправки контейнер `producer` завершится, а Kafka, Zookeeper и Kafka UI продолжат работать.

Если нужно запустить все в фоне:

```bash
docker compose up --build -d
```

Посмотреть логи producer:

```bash
docker compose logs producer
```

## Как открыть Kafka UI

Откройте в браузере:

```text
http://localhost:8080
```

В Kafka UI выберите кластер `lab-kafka-cluster`.

## Как проверить сообщения в Topic1 и Topic2

Через Kafka UI:

1. Откройте `http://localhost:8080`.
2. Выберите кластер `lab-kafka-cluster`.
3. Перейдите в раздел `Topics`.
4. Откройте `Topic1`.
5. Перейдите во вкладку с сообщениями/messages и убедитесь, что там есть JSON-сообщения из CSV.
6. Повторите то же самое для `Topic2`.

Через консоль Docker можно проверить список топиков:

```bash
docker compose exec kafka-broker1 kafka-topics --bootstrap-server kafka-broker1:29092 --list
```

Прочитать сообщения из `Topic1`:

```bash
docker compose exec kafka-broker1 kafka-console-consumer --bootstrap-server kafka-broker1:29092 --topic Topic1 --from-beginning --timeout-ms 10000
```

Прочитать сообщения из `Topic2`:

```bash
docker compose exec kafka-broker1 kafka-console-consumer --bootstrap-server kafka-broker1:29092 --topic Topic2 --from-beginning --timeout-ms 10000
```

## Как остановить проект

Остановить контейнеры:

```bash
docker compose down
```

Остановить контейнеры и удалить volumes, если нужно полностью очистить состояние:

```bash
docker compose down -v
```

## Какие скриншоты сделать для отчета/Moodle

Сделайте такие скриншоты:

1. Терминал с командой `docker compose up --build` и логами успешного запуска.
2. Логи producer, где видно отправку строк в `Topic1` и `Topic2`.
3. Kafka UI на `http://localhost:8080`, где виден кластер `lab-kafka-cluster`.
4. Список топиков в Kafka UI, где есть `Topic1` и `Topic2`.
5. Сообщения в `Topic1` в Kafka UI.
6. Сообщения в `Topic2` в Kafka UI.

## Настройки producer

Producer использует переменные окружения из `docker-compose.yml`:

- `KAFKA_BOOTSTRAP_SERVERS=kafka-broker1:29092,kafka-broker2:29093`
- `KAFKA_TOPICS=Topic1,Topic2`
- `CSV_FILE=/app/data.csv`

По умолчанию producer ждет Kafka до 120 секунд. Это можно изменить переменной:

```yaml
KAFKA_MAX_WAIT_SECONDS: 180
```
