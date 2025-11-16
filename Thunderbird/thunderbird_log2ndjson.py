import json
import os
import re
from datetime import datetime

INPUT_FILE = r"C:\Users\nikhi\loghub\Thunderbird\Thunderbird_2k.log"
OUTPUT_FILE = r"C:\Users\nikhi\loghub\Thunderbird\thunderbird_bulk.ndjson"

LOG_LINE_REGEX = re.compile(r"""
    ^-\s*
    (?P<epoch>\d+)\s+
    (?P<date>\d{4}\.\d{2}\.\d{2})\s+
    (?P<host>\S+)\s+
    (?P<rest>.*)$
""", re.VERBOSE)

def parse_line(line):
    match = LOG_LINE_REGEX.match(line)
    if not match:
        return None

    epoch = match.group("epoch")
    date_field = match.group("date")
    host = match.group("host")
    message = match.group("rest")

    # Convert epoch to ISO8601
    try:
        ts_iso = datetime.utcfromtimestamp(int(epoch)).isoformat() + "Z"
    except:
        ts_iso = None

    return {
        "timestamp_epoch": epoch,
        "timestamp_iso": ts_iso,
        "date_field": date_field,
        "host": host,
        "message": message,
        "raw_line": line.strip(),
    }


def convert():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(INPUT_FILE, "r", encoding="utf-8", errors="ignore") as fin, \
         open(OUTPUT_FILE, "w", encoding="utf-8") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue

            doc = parse_line(line)
            if doc is None:
                continue

            # Bulk index action
            fout.write(json.dumps({ "index": { "_index": "thunderbird" } }) + "\n")
            fout.write(json.dumps(doc) + "\n")

    print(f"✔ Conversion complete!")
    print(f"✔ Output saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    convert()
