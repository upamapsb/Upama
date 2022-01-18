from datetime import datetime


def export_timestamp(filename):
    with open(filename, "w") as f:
        f.write(datetime.utcnow().replace(microsecond=0).isoformat())
