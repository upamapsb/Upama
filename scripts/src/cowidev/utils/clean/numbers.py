import re


def clean_count(count):
    count = re.sub(r"[^0-9]", "", count)
    count = int(count)
    return count
