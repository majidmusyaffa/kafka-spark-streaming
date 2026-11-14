# Streaming for Data Pipeline: Kafka + Spark Structured Streaming

## Deskripsi dan arsitektur

Project ini membaca event JSON dari Kafka, memvalidasinya dengan PySpark
Structured Streaming, lalu merutekan event valid dan invalid ke topic terpisah.

```text
producer -> Kafka: transactions -> Spark Structured Streaming
                                -> Kafka: transactions_valid
                                -> Kafka: transactions_dlq
```

## Prasyarat

- Docker dan Docker Compose
- Python 3.9+ untuk producer

Install dependency producer dan jalankan infrastruktur:

```bash
pip install -r requirements.txt
docker compose up -d
```

## Menjalankan

Buat topic input dan output (perintah aman dijalankan berulang kali):

```bash
for topic in transactions transactions_valid transactions_dlq; do
  docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
    --create --if-not-exists --topic "$topic" \
    --bootstrap-server kafka:29092 \
    --partitions 1 --replication-factor 1
done
```

Jalankan Spark job di terminal pertama. Kafka connector disediakan melalui
`--packages`; koneksi dari container Spark ke Kafka menggunakan `kafka:29092`.

```bash
docker compose exec \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e CHECKPOINT_LOCATION=/app/streaming/checkpoints/transactions \
  -e DEDUP_DB_PATH=/app/streaming/state/transactions_seen.sqlite \
  spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /app/streaming/spark_streaming_job.py
```

Jalankan producer di terminal kedua:

```bash
python producer/producer.py
```

Untuk memeriksa hasil, baca topic di terminal lain:

```bash
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic transactions_valid \
  --from-beginning

docker exec -it kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic transactions_dlq \
  --from-beginning
```

## Validasi

Job mendefinisikan schema JSON, lalu memberi setiap event kolom `is_valid` dan
`error_reason`. Event valid memiliki `error_reason: null`; event invalid berisi
satu atau lebih alasan yang dipisahkan koma.

1. Field wajib `user_id`, `amount`, dan `timestamp` harus ada dan tidak null.
2. Tipe JSON divalidasi (termasuk format timestamp).
3. `amount` harus berada dalam rentang 1–10.000.000.
4. `source` harus salah satu dari `mobile`, `web`, atau `pos`.
5. Duplikat ditentukan oleh pasangan `user_id` + `timestamp`.

Event invalid tetap memuat `raw_message` agar payload aslinya dapat diperiksa di
DLQ. State deduplikasi disimpan di SQLite dan lokasi file-nya dapat diatur dengan
`DEDUP_DB_PATH`; gunakan lokasi pada volume persisten saat menjalankan job.
Checkpoint Spark juga disimpan terpisah pada `CHECKPOINT_LOCATION`.

Kafka sink bersifat at-least-once. Jika proses gagal ketika sedang menulis ke
topic output, sebagian event dari micro-batch dapat terkirim ulang saat recovery.

## Tumbling Window Monitoring

Selain validasi dan routing, job menjalankan query monitoring terpisah yang
menghitung **total transaksi per window** menggunakan tumbling window:

- **Window size**: 1 menit (`window(timestamp, "1 minute")`).
- **Event time**: agregasi berdasarkan kolom `timestamp` dari event.
- **Watermark**: 3 menit. Event yang tiba lebih dari 3 menit setelah event
  time-nya dianggap **late** dan otomatis di-drop dari agregasi oleh Spark
  (mekanisme deteksi late event).
- **Output**: ditampilkan ke console dengan `outputMode("append")`, berisi
  `window_start`, `window_end`, dan `total_transactions`.

Checkpoint monitoring disimpan terpisah di
`/tmp/spark-checkpoint/transactions-monitoring`.