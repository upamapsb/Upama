import argparse


CHOICES = [
    "etl",
    "explorer-file",
]


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Execute COVID-19 excess mortality data collection pipeline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "step",
        choices=CHOICES,
        default="etl",
        help=(
            "Choose a step: 1) `etl` to get all data and DS ready file,  2) `explorer-file` to generate a"
            " explorer-friendly file"
        ),
    )
    args = parser.parse_args()
    return args
