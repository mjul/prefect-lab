# Prefect Lab
Taking Prefect for a spin building a Python pipeline.

## Specification
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

### Collect Currency Pairs
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


## Tech Stack
- Python
- Prefect <https://github.com/PrefectHQ/prefect>

