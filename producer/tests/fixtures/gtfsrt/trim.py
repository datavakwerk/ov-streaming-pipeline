"""Trim freshly recorded feeds to the first N entities so fixtures stay small in git."""

from pathlib import Path

from google.transit import gtfs_realtime_pb2 as gtfs

HERE = Path(__file__).parent


def trim(src: Path, dst: Path, n: int) -> None:
    feed = gtfs.FeedMessage()
    feed.ParseFromString(src.read_bytes())
    out = gtfs.FeedMessage()
    out.header.CopyFrom(feed.header)
    for entity in list(feed.entity)[:n]:
        out.entity.add().CopyFrom(entity)
    dst.write_bytes(out.SerializeToString())
    print(f"{dst.name}: {len(out.entity)} entities")


if __name__ == "__main__":
    trim(HERE / "vehicle_positions.full.pb", HERE / "vehicle_positions.pb", 200)
    trim(HERE / "trip_updates.full.pb", HERE / "trip_updates.pb", 300)
