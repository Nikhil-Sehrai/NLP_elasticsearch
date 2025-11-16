import json
import os
import re
from datetime import datetime

INPUT_FILE = r"C:\Users\nikhi\loghub\Apache\Apache_2k.log"
OUTPUT_FILE = r"C:\Users\nikhi\loghub\Apache\apache_bulk.ndjson"

# Example line:
# [Sun Dec 04 04:47:44 2005] [notice] workerEnv.init() ok /etc/httpd/conf/workers2.properties
LOG_LINE_REGEX = re.compile(
    r"""
    ^\[
    (?P<weekday>\w{3})\s+
    (?P<month>\w{3})\s+
    (?P<day>\d{2})\s+
    (?P<time>\d{2}:\d{2}:\d{2})\s+
    (?P<year>\d{4})
    \]\s+
    \[(?P<level>[^\]]+)\]\s+
    (?P<rest>.*)
    $
    """,
    re.VERBOSE,
)

def parse_line(line):
    match = LOG_LINE_REGEX.match(line)
    if not match:
        return None

    weekday = match.group("weekday")
    month = match.group("month")
    day = match.group("day")
    time_str = match.group("time")
    year = match.group("year")
    level = match.group("level")
    rest = match.group("rest")

    # Build a datetime from the pieces (no timezone info in log, assume naive)
    dt_str = f"{weekday} {month} {day} {time_str} {year}"
    try:
        dt = datetime.strptime(dt_str, "%a %b %d %H:%M:%S %Y")
        ts_epoch = int(dt.timestamp())
        ts_iso = dt.isoformat() + "Z"
    except Exception:
        ts_epoch = None
        ts_iso = None

    return {
        "timestamp_epoch": ts_epoch,
        "timestamp_iso": ts_iso,
        "log_level": level,
        "message": rest,
        "raw_line": line.strip(),
    }


def convert():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(INPUT_FILE, "r", encoding="utf-8", errors="ignore") as fin, \
         open(OUTPUT_FILE, "w", encoding="utf-8") as fout:

        for line in fin:
            line = line.rstrip("\n")
            if not line.strip():
                continue

            doc = parse_line(line)
            if doc is None:
                # Skip lines that don't match the expected pattern
                continue

            # Bulk index action for Elasticsearch-style bulk API
            fout.write(json.dumps({"index": {"_index": "apache"}}) + "\n")
            fout.write(json.dumps(doc) + "\n")

    print("✔ Conversion complete!")
    print(f"✔ Output saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    convert()
