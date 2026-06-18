from prometheus_client import Counter, Gauge

events_ingested = Counter("events_ingested_total", "Transactions accepted by the API")
events_processed = Counter("events_processed_total", "Transactions successfully persisted")
events_duplicate = Counter("events_duplicate_total", "Duplicate transactions skipped")
events_failed = Counter("events_failed_total", "Transactions dead-lettered after exhausting retries")
queue_lag = Gauge("queue_lag_messages", "Pending (delivered but unacked) messages in the consumer group")
