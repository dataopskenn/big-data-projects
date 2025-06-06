# ----------------------------------------------------------------------------------
# FILE: main.py
# Entrypoint for CLI usage. Avoids double execution by guarding with __name__ == '__main__'.
# ----------------------------------------------------------------------------------

# main.py
import argparse
from pipeline import run_etl



def main():
    """
    Parses command-line arguments and calls ETL pipeline
    """
    parser = argparse.ArgumentParser(description="NYC Taxi ETL Pipeline")
    parser.add_argument("--year", type=int, required=True, help="Year of data")
    parser.add_argument("--month", type=int, help="Single month to process")
    parser.add_argument("--all-months", action="store_true", help="Process all 12 months")

    args = parser.parse_args()

    if args.all_months:
        for month in range(1, 13):
            run_etl(args.year, month)
    elif args.month:
        run_etl(args.year, args.month)
    else:
        print("You must provide either --month or --all-months")


# This prevents execution during import by other tools or DAGs.
if __name__ == "__main__":
    main()
