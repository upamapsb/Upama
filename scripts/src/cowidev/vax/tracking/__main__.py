import os
import argparse

from cowidev.vax.tracking.countries import countries_missing, country_updates_summary
from cowidev.vax.tracking.vaccines import vaccines_missing, vaccines_comparison_with_who


def _parse_args():
    parser = argparse.ArgumentParser(description="Get tracking information from dataset.")
    parser.add_argument(
        "mode",
        choices=[
            "countries-missing",
            "countries-last-updated",
            "countries-least-updated",
            "countries-least-updatedfreq",
            "vaccines-missing",
            "vaccines-missing-who",
            "report",
        ],
        default="countries-last-updated",
        help=(
            "Choose a step: 1) `countries-missing` will get table with countries not included in dataset, 2) "
            "`countries-last-updated` will get table with countries last updated, 3) `countries-least-updated` will "
            "get table with countries least updated, 4) `countries-least-updatedfreq` will g et table with countries "
            "least frequently updated (ratio of number of updates and time since first observation), "
            "5) `vaccines-missing` will get table with missing vaccines: Unapproved (but tracked) and Untracked (but "
            "approved), 6) `vaccines-missing-who` get comparison with WHO vaccine record."
        ),
    )
    parser.add_argument("--who", action="store_true", help="Display country names by WHO.")
    parser.add_argument("--to-csv", action="store_true", help="Export outputs to CSV.")
    parser.add_argument(
        "--vax",
        action="store_true",
        help="Add vaccines missing (compared to WHO's data).",
    )
    args = parser.parse_args()
    return args


def export_to_csv(df, filename):
    df.to_csv(filename, index=False)
    filename = os.path.abspath(filename)
    print(f"Data exported to {filename}")


def main():
    args = _parse_args()
    if args.mode == "countries-missing":
        print("-- Missing countries... --\n")
        df = countries_missing()
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
        if args.to_csv:
            export_to_csv(df, filename="countries-missing.tmp.csv")
    if args.mode == "countries-last-updated":
        print("-- Last updated countries... --")
        df = country_updates_summary()
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
        if args.to_csv:
            export_to_csv(df, filename="countries-last-updated.tmp.csv")
    if args.mode == "countries-least-updated":
        print("-- Least updated countries... --")
        df = country_updates_summary(sortby_counts=True)
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
        if args.to_csv:
            export_to_csv(df, filename="countries-least-updated.tmp.csv")
    if args.mode == "countries-least-updatedfreq":
        print("-- Least frequently-updated countries... --")
        df = country_updates_summary(sortby_updatefreq=True, who=args.who, vaccines=args.vax)
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
        if args.to_csv:
            export_to_csv(df, filename="countries-least-updatedfreq.tmp.csv")
    if args.mode == "vaccines-missing":
        print("-- Missing vaccines... --")
        df = vaccines_missing(verbose=True)
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
        if args.to_csv:
            export_to_csv(df, filename="vaccines-missing-who.tmp.csv")
    if args.mode == "vaccines-missing-who":
        print("-- Missing vaccines (WHO)... --")
        df = vaccines_comparison_with_who()
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
        if args.to_csv:
            export_to_csv(df, filename="vaccines-missing-who.tmp.csv")
    if args.mode == "report":
        print("-- Full report of vax data... --")
        df = country_updates_summary(sortby_updatefreq=True, who=True, vaccines=True, metric_counts=True)
        print(df)
        print("----------------------------\n----------------------------\n----------------------------\n")
        if args.to_csv:
            export_to_csv(df, filename="cowid-vax-track.report.tmp.csv")


if __name__ == "__main__":
    main()
