# Streaming for Data Pipeline: Kafka + Spark Structured Streaming

## Deskripsi
TODO: jelaskan tujuan project dan alur data secara singkat.

## Arsitektur
TODO: gambar atau diagram teks sederhana, misalnya:
producer -> Kafka (transactions) -> Spark Streaming -> transactions_valid / transactions_dlq

## Struktur Folder
├── docker-compose.yml
├── producer/producer.py
├── streaming/spark_streaming_job.py
├── screenshots/
└── README.md


## Prasyarat
- Docker dan Docker Compose
- Python 3.9+ dan `pip install kafka-python`

## Cara Menjalankan
1. Jalankan infrastruktur: `docker compose up -d`
2. Buat topic: TODO (tulis perintahnya)
3. Jalankan producer: `python producer/producer.py`
4. Jalankan streaming job: TODO (tulis perintah spark-submit)

## Desain Producer
TODO: jenis event valid, invalid, late, dan cara menyisipkannya.

## Validasi di Streaming Job
TODO: jelaskan 5 validasi dan kolom `is_valid`, `error_reason`.

## Watermark dan Window
TODO: watermark 3 menit, tumbling window 1 menit.

## Screenshot
- Producer: `screenshots/producer.png`
- Streaming console: `screenshots/console.png`
- Topic valid dan DLQ: `screenshots/topics.png`

## Catatan
TODO: kendala yang dihadapi dan cara mengatasinya.