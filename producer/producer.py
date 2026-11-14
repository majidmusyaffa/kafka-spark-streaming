import json
import random
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

TOPIC = "transactions"
BOOTSTRAP = "localhost:9092"
SOURCES = ["mobile", "web", "pos"]

TOTAL_EVENTS = 200
MIN_INTERVAL = 1
MAX_INTERVAL = 2

SCHEDULE = {
    10: "invalid:missing_field",
    14: "invalid:null_amount",
    18: "invalid:wrong_type",
    22: "duplicate",
    26: "invalid:negative_amount",
    28: "late",
    32: "invalid:too_large_amount",
    36: "invalid:bad_source",
    40: "late",
    44: "invalid:bad_timestamp",
    48: "duplicate",
    80: "late",
    100: "duplicate",
    120: "invalid:bad_source",
    140: "late",
    160: "duplicate",
    180: "invalid:wrong_type",
    195: "late",
}


def now_iso(offset_minutes=0):
    """Return UTC time minus offset_minutes as ISO string ending with 'Z'.
    contoh: 2026-10-25T08:05:12Z"""
    event_time = datetime.now(timezone.utc) - timedelta(minutes=offset_minutes)
    return event_time.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_valid_event():

    user_id = f"U{random.randint(0, 99999):05d}"
    amount = random.randint(1, 10_000_000)
    timestamp = now_iso()
    source = random.choice(SOURCES)

    return {
        "user_id":user_id,
        "amount":amount,
        "timestamp":timestamp,
        "source":source
    }

def make_invalid_event(kind):
    """Invalid event with different kind of error."""
    event = make_valid_event()
    if kind == "missing_field":
        event.pop('user_id')
    elif kind == "bad_source":
        event['source'] = 'atm'
    elif kind == "null_amount":
        event['amount'] = None
    elif kind == "wrong_type":
        event['amount'] = "seratus ribu"
    elif kind == "negative_amount":
        event['amount'] = event['amount'] * -1
    elif kind == "too_large_amount":
        event['amount'] = event['amount'] * 10_000_000_000
    elif kind == "bad_timestamp":
        event['timestamp'] = "bukan-tanggal"
    else:
        raise ValueError(f"unknown kind: {kind}")
    return event

def make_late_event():
    """Valid event whose timestamp is 5-10 minutes in the past."""
    event = make_valid_event()
    event["timestamp"] = now_iso(random.randint(5,10))
    return event

def main():
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
    last_valid = None
    try:
        while True:
            for count in range(1, TOTAL_EVENTS + 1):
                kind = SCHEDULE.get(count, "valid")

                if kind == "valid":
                    event = make_valid_event()
                    last_valid = event
                    label = "VALID"
                elif kind.startswith("invalid:"):
                    _, error_kind = kind.split(":")
                    event = make_invalid_event(error_kind)
                    label = f"INVALID: {error_kind}"
                elif kind == "late":
                    event = make_late_event()
                    label = "LATE"
                elif kind == "duplicate":
                    event = last_valid
                    label = "DUPLICATE"
                else:
                    raise ValueError(f"unknown schedule kind: {kind}")

                producer.send(TOPIC, json.dumps(event).encode("utf-8"))
                print(f"[{label}] #{count} {event}")
                time.sleep(random.uniform(MIN_INTERVAL, MAX_INTERVAL))
    except KeyboardInterrupt:
        print("stopped by user")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()