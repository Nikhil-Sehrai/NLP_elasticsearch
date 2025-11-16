import json
import os
import re
from datetime import datetime, timedelta

INPUT_FILE = r"C:\Users\nikhi\loghub\HDFS\HDFS_2k.log"
OUTPUT_FILE = r"C:\Users\nikhi\loghub\HDFS\hdfs_bulk.ndjson"

# Example line:
# 081109 203615 148 INFO dfs.DataNode$PacketResponder: PacketResponder 1 for block blk_38865049064139660 terminating
LOG_LINE_REGEX = re.compile(
    r"""
    ^(?P<date>\d{6})\s+                # yymmdd
    (?P<time>\d{6})\s+                 # hhmmss
    (?P<millis>\d+)\s+                 # milliseconds (or similar)
    (?P<level>[A-Z]+)\s+               # log level
    (?P<component>\S+):\s+             # logger / component ending with colon
    (?P<message>.*)$                   # rest of line
    """,
    re.VERBOSE,
)


def parse_line(line):
    match = LOG_LINE_REGEX.match(line)
    if not match:
        return None

    date_str = match.group("date")      # yymmdd
    time_str = match.group("time")      # hhmmss
    millis_str = match.group("millis")
    level = match.group("level")
    component = match.group("component")
    message = match.group("message")

    # Build ISO8601 timestamp, assuming years are 20yy
    try:
        yy = int(date_str[0:2])
        mm = int(date_str[2:4])
        dd = int(date_str[4:6])

        hh = int(time_str[0:2])
        mi = int(time_str[2:4])
        ss = int(time_str[4:6])

        base_dt = datetime(2000 + yy, mm, dd, hh, mi, ss)
        millis = int(millis_str)
        dt = base_dt + timedelta(milliseconds=millis)

        ts_iso = dt.isoformat() + "Z"
        timestamp_epoch = int(dt.timestamp())
    except Exception:
        ts_iso = None
        timestamp_epoch = None

    return {
        "timestamp_epoch": timestamp_epoch,
        "timestamp_iso": ts_iso,
        "date_raw": date_str,
        "time_raw": time_str,
        "millis": millis_str,
        "level": level,
        "component": component,
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

            # Bulk index action for Elasticsearch
            fout.write(json.dumps({"index": {"_index": "hdfs"}}) + "\n")
            fout.write(json.dumps(doc) + "\n")

    print("✔ Conversion complete!")
    print(f"✔ Output saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    convert()
