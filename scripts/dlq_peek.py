"""Pretty-print `rpk topic consume --format json` output of a DLQ topic (used by make dlq-peek)."""

import json
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        print(line)
        continue
    headers = {h["key"]: h.get("value", "") for h in record.get("headers", [])}
    value = record.get("value") or ""
    print(
        f"  p{record['partition']}@{record['offset']}  key={record.get('key')!s:<28} "
        f"stage={headers.get('stage', '?'):<7} error={headers.get('error', '')!r}"
    )
    print(
        f"      source={headers.get('source_topic', '?')} "
        f"failed_at={headers.get('failed_at', '?')} "
        f"value={value[:80]!r}{'…' if len(value) > 80 else ''}"
    )
