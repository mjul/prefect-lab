"""
Tests for the currency exchange rate pipeline.
"""
import unittest
import os
from pathlib import Path

from src.pipeline import (
    currency_exchange_rate_pipeline,
    download_currency_data_flow,
    clean_up_currency_data_flow,
    CURRENCY_PAIRS,
    DATA_DIR,
)


class TestPipeline(unittest.TestCase):
    """Test cases for the currency exchange rate pipeline."""

    def test_download_currency_data(self):
        """Test that the download_currency_data_flow function works correctly."""
        # Run the flow
        downloaded_files = download_currency_data_flow()

        # Check that the correct number of files were downloaded
        self.assertEqual(len(downloaded_files), len(CURRENCY_PAIRS))

        # Check that all files exist
        for file_path in downloaded_files:
            self.assertTrue(file_path.exists())

            # Check that the file is not empty
            self.assertGreater(file_path.stat().st_size, 0)

            # Check that the file has the correct name format
            file_name = file_path.name
            self.assertTrue(file_name.startswith("ECB_EUR_"))
            self.assertTrue(file_name.endswith(".csv"))

            # Extract the currency from the file name
            currency = file_name.replace("ECB_EUR_", "").replace(".csv", "")
            self.assertIn(currency, CURRENCY_PAIRS)

    def test_full_pipeline(self):
        """Test that the full pipeline works correctly and produces all expected output files."""
        # Run the pipeline
        result = currency_exchange_rate_pipeline()

        # Check that all expected output files were produced

        # 1. Check downloaded files
        downloaded_files = result["downloaded_files"]
        self.assertEqual(len(downloaded_files), len(CURRENCY_PAIRS))
        for file_path in downloaded_files:
            self.assertTrue(file_path.exists())
            self.assertGreater(file_path.stat().st_size, 0)

        # 2. Check cleaned files
        cleaned_files = result["cleaned_files"]
        self.assertEqual(len(cleaned_files), len(CURRENCY_PAIRS))
        for file_path in cleaned_files:
            self.assertTrue(file_path.exists())
            self.assertGreater(file_path.stat().st_size, 0)

            # Check that the file has the correct name format
            file_name = file_path.name
            self.assertFalse(file_name.startswith("ECB_"))  # Should not have ECB_ prefix
            self.assertTrue(file_name.startswith("EUR_"))   # Should start with EUR_
            self.assertTrue(file_name.endswith(".csv"))     # Should end with .csv

            # Extract the currency from the file name
            currency = file_name.replace("EUR_", "").replace(".csv", "")
            self.assertIn(currency, CURRENCY_PAIRS)

        # 3. Check pairs file
        pairs_file = result["pairs_file"]
        self.assertTrue(pairs_file.exists())
        self.assertGreater(pairs_file.stat().st_size, 0)

        # 4. Check dates file
        dates_file = result["dates_file"]
        self.assertTrue(dates_file.exists())
        self.assertGreater(dates_file.stat().st_size, 0)

        # 5. Check monthly stats files
        monthly_stats_files = result["monthly_stats_files"]
        self.assertGreaterEqual(len(monthly_stats_files), 1)  # At least one file should be produced
        for file_path in monthly_stats_files:
            self.assertTrue(file_path.exists())
            self.assertGreater(file_path.stat().st_size, 0)

            # Check that the file has the correct name format
            file_name = file_path.name
            self.assertTrue("_monthly_stats.csv" in file_name)

        # 6. Check missing data files
        missing_data_files = result["missing_data_files"]
        self.assertGreaterEqual(len(missing_data_files), 1)  # At least one file should be produced
        for file_path in missing_data_files:
            self.assertTrue(file_path.exists())

            # Check that the file has the correct name format
            file_name = file_path.name
            self.assertTrue("_missing_data.csv" in file_name)

        # 7. Check aggregate missing data file
        aggregate_file = result["aggregate_file"]
        self.assertTrue(aggregate_file.exists())

        print("\nAll pipeline output files were produced successfully:")
        print(f"- Downloaded files: {[f.name for f in downloaded_files]}")
        print(f"- Cleaned files: {[f.name for f in cleaned_files]}")
        print(f"- Pairs file: {pairs_file.name}")
        print(f"- Dates file: {dates_file.name}")
        print(f"- Monthly stats files: {[f.name for f in monthly_stats_files]}")
        print(f"- Missing data files: {[f.name for f in missing_data_files]}")
        print(f"- Aggregate missing data file: {aggregate_file.name}")


if __name__ == "__main__":
    unittest.main()
