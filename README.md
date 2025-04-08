# Prefect Lab: Currency Exchange Rate Pipeline

This project implements a data pipeline using Prefect to process currency exchange rate data from the European Central Bank (ECB).

## Overview

The pipeline consists of the following steps:

1. **Download Data**: Downloads daily currency exchange rate data for EUR/USD, EUR/SEK, EUR/NOK, and EUR/DKK from the ECB API.
2. **Collect Currency Pairs**: Extracts currency pairs from the downloaded data and saves them to `pairs.csv`.
3. **Collect Dates**: Extracts dates from the downloaded data and saves them to `dates.csv`.
4. **Compute Stats**: Calculates monthly statistics (high, low, average) for each currency pair and saves them to `<PAIR>_monthly_stats.csv` files.
5. **Identify Missing Data**: Identifies months with missing data for each currency pair and saves them to `<PAIR>_missing_data.csv` files.
6. **Aggregate Missing Data**: Combines all missing data into a single `missing_data.csv` file.

## Project Structure

```
prefect-lab/
├── data/                  # Directory for input/output data
├── src/                   # Source code
│   ├── __init__.py        # Package initialization
│   └── pipeline.py        # Pipeline implementation
├── tests/                 # Tests
│   ├── __init__.py        # Test package initialization
│   └── test_pipeline.py   # Pipeline tests
├── pyproject.toml         # Project configuration
└── README.md              # This file
```

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/prefect-lab.git
   cd prefect-lab
   ```

2. Install the package and its dependencies:
   ```
   pip install -e .
   ```

## Running the Pipeline

To run the complete pipeline:

```python
from src.pipeline import currency_exchange_rate_pipeline

# Run the pipeline
result = currency_exchange_rate_pipeline()
```

Or you can run the pipeline script directly:

```
python -m src.pipeline
```

## Running the Tests

To run the tests:

```
pytest
```

Or to run a specific test:

```
pytest tests/test_pipeline.py::TestPipeline::test_full_pipeline
```

## Output Files

The pipeline produces the following output files in the `data` directory:

- `ECB_EUR_USD.csv`, `ECB_EUR_SEK.csv`, etc.: Raw data downloaded from the ECB API
- `pairs.csv`: List of currency pairs
- `dates.csv`: List of dates
- `EUR_USD_monthly_stats.csv`, etc.: Monthly statistics for each currency pair
- `EUR_USD_missing_data.csv`, etc.: Missing data for each currency pair
- `missing_data.csv`: Aggregate missing data for all currency pairs

## Dependencies

- Python 3.8+
- Prefect 2.0+
- Pandas 1.3+
- Requests 2.25+

## Original Specification

We will use Prefect to build a data ingress pipeline, 
a directed acyclic graph. All data is stored in CSV files in the data folder.

The task is to download and process currency exchange rates from the ECB.
See the API documentation here: <https://data.ecb.europa.eu/help/api/overview>

### Download Data
First job is to download the daily data each of the pairs EUR/USD, EUR/SEK, EUR/NOK, EUR/SEK.
Save these as CSV named "ECB_EUR_USD.csv" etc. 
The source data is available at URLs like this one <https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata>
Do it with fan-out if possible.

Each of these files are read and parsed and the records are to be fed to 
a number of jobs. Each job produces an output as a file. 
The job is resumable meaning it only runs if its output does not exist yet 
or if its inputs were updated. 

### Collect Currency Pairs
The next job simply collects the currency pairs mentioned in the "ECB_" files downloaded above.
e.g. if we have a price for EUR/USD on some day it will collect EUR/USD. Write this to "pairs.csv".
Create a job for each of the ECB data files above.

### Collect Dates
The next job collects the dates mentioned. Dates only, no timezone.
Dates are treated like a set. It saves these in the file "dates.csv".
Create a job for each of the ECB data files above.

### Compute Stats
The fourth job computes the high, low and average price for each pair
for each month, e.g. EUR/USD is saved to "EUR_USD_monthly_stats.csv" (high and low are maximum and minimum).
Create a job for each of the ECB data files above.

### Identify Missing Data
The fifth job reads the "pairs.csv" and "dates.csv" and calculates the calendar months from
the minimum date to the maximum date, both months inclusive. 
Then for each currency pair i pairs.csv it spawns a task to read the ..._monthly_stats.csv file 
for that pair to verify that all the months just calculated are present. If any months are missing
collect these and the name of the pair and add them to a file for that pair, e.g. "EUR_USD_missing_data.csv"

### Aggregate the Missing Data into One File
Final job, wait for the missing data files for the pairs to be generated, then 
create an aggregate file, "missing_data.csv" with all the months and pairs that from the _missing_data.csv files
generated in the step above and all the pairs in the "pairs.csv" file.
