from prometheus_client import Counter, Gauge

CONSUMED = Counter("archiver_messages_total", "Messages consumed", ["topic"])
OBJECTS = Counter("archiver_objects_total", "Objects written to the archive", ["topic"])
BYTES = Counter("archiver_bytes_total", "Compressed bytes written")
PENDING = Gauge("archiver_pending_messages", "Messages buffered, not yet flushed")
COMMITS = Counter("archiver_commits_total", "Offset commits after successful flushes")
