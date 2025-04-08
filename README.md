# Prefect Lab
Taking Prefect for a spin building a Python pipeline.

## Specification
We will use Prefect to build a data ingress pipeline, 
a directed acyclic graph. All data is stored in CSV files in the data folder.

The task is to process a CSV file of currency prices over time. 
This is the input.

The file is read and parsed and the records are to be fed to 
a number of jobs. Each job produces an output as a file. 
The job is resumable meaning it only runs if its output does not exist yet 
or if its inputs were updated.

The first job simply collects the currency pairs mentioned overall,
e.g. if we have a price for EUR/USD on some day it will collect EUR/USD. Write this to "pairs.csv".

The second job collects the dates mentioned. Dates only, no timezone.
Dates are treated like a set. It saves these in the file "dates.csv".

The third job splits the data by currency pair and produces a time series for each.
This data goes into a separate file named for the pair, e.g. "EUR_USD_prices.csv" for the
EUR/USD pair.

The fourth job computes the high, low and average price for each pair
for each day, e.g. EUR/USD is saved to "EUR_USD_stats.csv" (high and low are maximum and minimum).

## Tech Stack
- Python
- Prefect <https://github.com/PrefectHQ/prefect>

