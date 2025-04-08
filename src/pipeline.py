"""
Currency Exchange Rate Pipeline

This module contains the Prefect flow and tasks for processing ECB currency exchange rate data.
"""
import os
import csv
import requests
import pandas as pd
from datetime import datetime
from typing import List, Dict, Set, Tuple
from pathlib import Path

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

# Define constants
DATA_DIR = Path("data")
CURRENCY_PAIRS = ["USD", "SEK", "NOK", "DKK"]  # Base currency is EUR


@task(retries=3, retry_delay_seconds=5)
def download_currency_data(currency: str) -> Path:
    """
    Download currency exchange rate data from ECB for a specific currency pair.

    Args:
        currency: The currency code (e.g., USD, SEK)

    Returns:
        Path to the saved CSV file
    """
    # Ensure data directory exists
    DATA_DIR.mkdir(exist_ok=True)

    # Construct the output file path
    output_file = DATA_DIR / f"ECB_EUR_{currency}.csv"

    # Check if file already exists and is recent (less than 24 hours old)
    if output_file.exists():
        file_age = datetime.now().timestamp() - output_file.stat().st_mtime
        if file_age < 24 * 60 * 60:  # Less than 24 hours old
            print(f"Using existing file for EUR/{currency}: {output_file}")
            return output_file

    # Construct the URL for the ECB API
    url = f"https://data-api.ecb.europa.eu/service/data/EXR/D.{currency}.EUR.SP00.A?format=csvdata"

    print(f"Downloading EUR/{currency} data from {url}")

    # Download the data
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors

    # Save the data to a CSV file
    with open(output_file, "w", newline="") as f:
        f.write(response.text)

    print(f"Saved EUR/{currency} data to {output_file}")

    return output_file


@task
def clean_up_currency_data(file_path: Path) -> Path:
    """
    Clean up downloaded ECB CSV file by extracting only the required columns.

    Args:
        file_path: Path to the ECB CSV file

    Returns:
        Path to the cleaned CSV file
    """
    # Extract the currency from the file name
    file_name = file_path.name
    currency = file_name.replace("ECB_EUR_", "").replace(".csv", "")

    # Construct the output file path
    output_file = DATA_DIR / f"EUR_{currency}.csv"

    try:
        # Read the CSV file using pandas
        df = pd.read_csv(file_path)

        # Extract only the required columns
        if all(col in df.columns for col in ['CURRENCY', 'CURRENCY_DENOM', 'TIME_PERIOD', 'OBS_VALUE']):
            cleaned_df = df[['CURRENCY', 'CURRENCY_DENOM', 'TIME_PERIOD', 'OBS_VALUE']]

            # Rename columns
            cleaned_df = cleaned_df.rename(columns={
                'TIME_PERIOD': 'DATE',
                'OBS_VALUE': 'RATE'
            })

            # Write to CSV
            cleaned_df.to_csv(output_file, index=False)

            print(f"Saved cleaned data for EUR/{currency} to {output_file}")
        else:
            print(f"Warning: Required columns not found in {file_path}")
            return None
    except Exception as e:
        print(f"Error cleaning up data for {file_path}: {e}")
        return None

    return output_file


@task
def collect_currency_pairs(file_path: Path) -> Set[str]:
    """
    Extract currency pairs from a cleaned CSV file.

    Args:
        file_path: Path to the cleaned CSV file (EUR_*.csv)

    Returns:
        Set of currency pairs (e.g., "EUR_USD")
    """
    # Extract the currency pair from the file name
    file_name = file_path.name

    # The file name is in the format "EUR_USD.csv"
    currency_pair = file_name.replace(".csv", "")

    return {currency_pair}


@task
def write_currency_pairs_to_csv(currency_pairs: Set[str]) -> Path:
    """
    Write currency pairs to a CSV file.

    Args:
        currency_pairs: Set of currency pairs

    Returns:
        Path to the saved CSV file
    """
    # Ensure data directory exists
    DATA_DIR.mkdir(exist_ok=True)

    # Construct the output file path
    output_file = DATA_DIR / "pairs.csv"

    # Write the currency pairs to a CSV file
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["currency_pair"])  # Header
        for pair in sorted(currency_pairs):
            writer.writerow([pair])

    print(f"Saved currency pairs to {output_file}")

    return output_file


@flow(name="Collect Currency Pairs Flow")
def collect_currency_pairs_flow(input_files: List[Path]) -> Path:
    """
    Collect currency pairs from downloaded ECB CSV files.

    Args:
        input_files: List of paths to the downloaded CSV files

    Returns:
        Path to the saved currency pairs CSV file
    """
    # Extract currency pairs from each file
    all_currency_pairs = set()
    for file_path in input_files:
        pairs = collect_currency_pairs(file_path)
        all_currency_pairs.update(pairs)

    # Write the currency pairs to a CSV file
    output_file = write_currency_pairs_to_csv(all_currency_pairs)

    return output_file


@flow(name="Download Currency Data Flow")
def download_currency_data_flow() -> List[Path]:
    """
    Download currency exchange rate data for all specified currency pairs.

    Returns:
        List of paths to the downloaded CSV files
    """
    # Use fan-out to download data for each currency pair in parallel
    downloaded_files = []
    for currency in CURRENCY_PAIRS:
        file_path = download_currency_data(currency)
        downloaded_files.append(file_path)

    return downloaded_files


@flow(name="Clean Up Currency Data Flow")
def clean_up_currency_data_flow(input_files: List[Path]) -> List[Path]:
    """
    Clean up downloaded ECB CSV files by extracting only the required columns.

    Args:
        input_files: List of paths to the downloaded ECB CSV files

    Returns:
        List of paths to the cleaned CSV files
    """
    # Clean up each file
    cleaned_files = []
    for file_path in input_files:
        cleaned_file = clean_up_currency_data(file_path)
        if cleaned_file:
            cleaned_files.append(cleaned_file)

    return cleaned_files


@task
def collect_dates(file_path: Path) -> Set[str]:
    """
    Extract dates from a cleaned CSV file.

    Args:
        file_path: Path to the cleaned CSV file

    Returns:
        Set of dates (YYYY-MM-DD format)
    """
    dates = set()

    # Read the CSV file using pandas
    try:
        # Read the cleaned CSV file
        df = pd.read_csv(file_path)

        # In cleaned files, the date column is named 'DATE'
        if 'DATE' in df.columns:
            # Extract unique dates
            dates.update(df['DATE'].unique())
        else:
            print(f"Warning: No DATE column found in {file_path}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")

    return dates


@task
def write_dates_to_csv(dates: Set[str]) -> Path:
    """
    Write dates to a CSV file.

    Args:
        dates: Set of dates

    Returns:
        Path to the saved CSV file
    """
    # Ensure data directory exists
    DATA_DIR.mkdir(exist_ok=True)

    # Construct the output file path
    output_file = DATA_DIR / "dates.csv"

    # Write the dates to a CSV file
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date"])  # Header
        for date in sorted(dates):
            writer.writerow([date])

    print(f"Saved dates to {output_file}")

    return output_file


@flow(name="Collect Dates Flow")
def collect_dates_flow(input_files: List[Path]) -> Path:
    """
    Collect dates from downloaded ECB CSV files.

    Args:
        input_files: List of paths to the downloaded CSV files

    Returns:
        Path to the saved dates CSV file
    """
    # Extract dates from each file
    all_dates = set()
    for file_path in input_files:
        dates = collect_dates(file_path)
        all_dates.update(dates)

    # Write the dates to a CSV file
    output_file = write_dates_to_csv(all_dates)

    return output_file


@task
def compute_monthly_stats(file_path: Path) -> Path:
    """
    Compute monthly statistics (high, low, average) for a currency pair.

    Args:
        file_path: Path to the cleaned CSV file

    Returns:
        Path to the saved monthly stats CSV file
    """
    # Extract the currency pair from the file name
    file_name = file_path.name

    # The file name is in the format "EUR_USD.csv"
    currency_pair = file_name.replace(".csv", "")

    # Construct the output file path
    output_file = DATA_DIR / f"{currency_pair}_monthly_stats.csv"

    try:
        # Read the cleaned CSV file
        df = pd.read_csv(file_path)

        # In cleaned files, column names are standardized
        date_column = 'DATE'
        value_column = 'RATE'

        if date_column in df.columns and value_column in df.columns:
            # Convert date column to datetime
            df[date_column] = pd.to_datetime(df[date_column])

            # Extract year and month
            df['year'] = df[date_column].dt.year
            df['month'] = df[date_column].dt.month

            # Group by year and month and compute statistics
            monthly_stats = df.groupby(['year', 'month'])[value_column].agg(['min', 'max', 'mean']).reset_index()
            monthly_stats.columns = ['year', 'month', 'low', 'high', 'average']

            # Add month_str column (YYYY-MM format)
            monthly_stats['month_str'] = monthly_stats.apply(
                lambda row: f"{int(row['year'])}-{int(row['month']):02d}", axis=1
            )

            # Reorder columns
            monthly_stats = monthly_stats[['month_str', 'low', 'high', 'average']]

            # Write to CSV
            monthly_stats.to_csv(output_file, index=False)

            print(f"Saved monthly stats for {currency_pair} to {output_file}")
        else:
            print(f"Warning: Required columns not found in {file_path}")
            return None
    except Exception as e:
        print(f"Error computing monthly stats for {file_path}: {e}")
        return None

    return output_file


@flow(name="Compute Monthly Stats Flow")
def compute_monthly_stats_flow(input_files: List[Path]) -> List[Path]:
    """
    Compute monthly statistics for all currency pairs.

    Args:
        input_files: List of paths to the downloaded CSV files

    Returns:
        List of paths to the saved monthly stats CSV files
    """
    # Compute monthly stats for each file
    output_files = []
    for file_path in input_files:
        output_file = compute_monthly_stats(file_path)
        if output_file:
            output_files.append(output_file)

    return output_files


@task
def generate_expected_months(dates_file: Path) -> List[str]:
    """
    Generate a list of expected months from the minimum date to the maximum date.

    Args:
        dates_file: Path to the dates CSV file

    Returns:
        List of months in YYYY-MM format
    """
    # Read the dates CSV file
    with open(dates_file, "r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        dates = [row[0] for row in reader]

    if not dates:
        return []

    # Convert dates to datetime objects
    date_objects = [pd.to_datetime(date) for date in dates]

    # Find the minimum and maximum dates
    min_date = min(date_objects)
    max_date = max(date_objects)

    # Generate a list of all months between min_date and max_date
    expected_months = []
    current_date = pd.Timestamp(year=min_date.year, month=min_date.month, day=1)

    while current_date <= max_date:
        expected_months.append(current_date.strftime("%Y-%m"))
        # Move to the next month
        if current_date.month == 12:
            current_date = pd.Timestamp(year=current_date.year + 1, month=1, day=1)
        else:
            current_date = pd.Timestamp(year=current_date.year, month=current_date.month + 1, day=1)

    return expected_months


@task
def identify_missing_data_for_pair(currency_pair: str, expected_months: List[str]) -> Path:
    """
    Identify missing data for a currency pair.

    Args:
        currency_pair: Currency pair (e.g., "EUR_USD")
        expected_months: List of expected months

    Returns:
        Path to the saved missing data CSV file
    """
    # Construct the path to the monthly stats file
    monthly_stats_file = DATA_DIR / f"{currency_pair}_monthly_stats.csv"

    # Construct the output file path
    output_file = DATA_DIR / f"{currency_pair}_missing_data.csv"

    # If the monthly stats file doesn't exist, all months are missing
    if not monthly_stats_file.exists():
        print(f"Warning: Monthly stats file for {currency_pair} does not exist")
        with open(output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["currency_pair", "month"])
            for month in expected_months:
                writer.writerow([currency_pair, month])
        return output_file

    # Read the monthly stats file
    try:
        monthly_stats = pd.read_csv(monthly_stats_file)

        # Get the list of months in the monthly stats file
        available_months = set(monthly_stats["month_str"])

        # Find missing months
        missing_months = [month for month in expected_months if month not in available_months]

        # Write missing months to CSV
        with open(output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["currency_pair", "month"])
            for month in missing_months:
                writer.writerow([currency_pair, month])

        print(f"Saved missing data for {currency_pair} to {output_file}")
    except Exception as e:
        print(f"Error identifying missing data for {currency_pair}: {e}")
        return None

    return output_file


@flow(name="Identify Missing Data Flow")
def identify_missing_data_flow(pairs_file: Path, dates_file: Path) -> List[Path]:
    """
    Identify missing data for all currency pairs.

    Args:
        pairs_file: Path to the pairs CSV file
        dates_file: Path to the dates CSV file

    Returns:
        List of paths to the saved missing data CSV files
    """
    # Generate expected months
    expected_months = generate_expected_months(dates_file)

    # Read the pairs CSV file
    with open(pairs_file, "r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        currency_pairs = [row[0] for row in reader]

    # Identify missing data for each currency pair
    missing_data_files = []
    for currency_pair in currency_pairs:
        missing_data_file = identify_missing_data_for_pair(currency_pair, expected_months)
        if missing_data_file:
            missing_data_files.append(missing_data_file)

    return missing_data_files


@task
def aggregate_missing_data(missing_data_files: List[Path], pairs_file: Path) -> Path:
    """
    Aggregate missing data from all currency pairs into a single file.

    Args:
        missing_data_files: List of paths to the missing data CSV files
        pairs_file: Path to the pairs CSV file

    Returns:
        Path to the saved aggregate missing data CSV file
    """
    # Construct the output file path
    output_file = DATA_DIR / "missing_data.csv"

    # Read all currency pairs from pairs.csv
    with open(pairs_file, "r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        all_currency_pairs = set(row[0] for row in reader)

    # Collect all missing data
    all_missing_data = []

    for file_path in missing_data_files:
        try:
            with open(file_path, "r") as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                for row in reader:
                    if len(row) >= 2:
                        currency_pair, month = row[0], row[1]
                        all_missing_data.append((currency_pair, month))
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    # Write the aggregate missing data to a CSV file
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["currency_pair", "month"])

        # Sort by currency pair and month
        for currency_pair, month in sorted(all_missing_data):
            writer.writerow([currency_pair, month])

    print(f"Saved aggregate missing data to {output_file}")

    return output_file


@flow(name="Aggregate Missing Data Flow")
def aggregate_missing_data_flow(missing_data_files: List[Path], pairs_file: Path) -> Path:
    """
    Aggregate missing data from all currency pairs.

    Args:
        missing_data_files: List of paths to the missing data CSV files
        pairs_file: Path to the pairs CSV file

    Returns:
        Path to the saved aggregate missing data CSV file
    """
    return aggregate_missing_data(missing_data_files, pairs_file)


@flow(name="Currency Exchange Rate Pipeline")
def currency_exchange_rate_pipeline():
    """
    Main pipeline flow that orchestrates all steps.
    """
    # Step 1: Download currency data
    downloaded_files = download_currency_data_flow()

    # Step 2: Clean up downloaded data
    cleaned_files = clean_up_currency_data_flow(downloaded_files)

    # Step 3: Collect currency pairs
    pairs_file = collect_currency_pairs_flow(cleaned_files)

    # Step 4: Collect dates
    dates_file = collect_dates_flow(cleaned_files)

    # Step 5: Compute monthly stats
    monthly_stats_files = compute_monthly_stats_flow(cleaned_files)

    # Step 6: Identify missing data
    missing_data_files = identify_missing_data_flow(pairs_file, dates_file)

    # Step 7: Aggregate missing data
    aggregate_file = aggregate_missing_data_flow(missing_data_files, pairs_file)

    return {
        "downloaded_files": downloaded_files,
        "cleaned_files": cleaned_files,
        "pairs_file": pairs_file,
        "dates_file": dates_file,
        "monthly_stats_files": monthly_stats_files,
        "missing_data_files": missing_data_files,
        "aggregate_file": aggregate_file,
    }


if __name__ == "__main__":
    # Run the pipeline
    currency_exchange_rate_pipeline()
