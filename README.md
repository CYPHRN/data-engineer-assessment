# Automotive Industry Data Pipeline

Data pipeline that acquires, processes, transforms and loads data from three US government sources related to the automotive industry.

---

## What It Does

Downloads three public datasets, cleans them, transforms them into analytical outputs, and simulates loading into Azure SQL Database and Databricks Delta Lake.

### Data Sources

| Source                                                                             | What It Contains                   | Method             |
| ---------------------------------------------------------------------------------- | ---------------------------------- | ------------------ |
| [NHTSA](https://www.nhtsa.gov/nhtsa-datasets-and-apis)                             | Vehicle safety complaints          | Flat file download |
| [AFDC / DOE](https://developer.nrel.gov/docs/transportation/alt-fuel-stations-v1/) | Alternative fuel station locations | NREL API           |
| [EPA](https://www.fueleconomy.gov/feg/download.shtml)                              | Vehicle fuel economy data          | CSV download       |

---

## Project Structure

```
├── scripts/
│   ├── 01_data_acquisition.py      # Downloads all three datasets
│   ├── 02_data_processing.py       # Cleans and filters the raw data
│   ├── 03_data_transformation.py   # Answers the 4 analysis questions
│   └── 04_data_loading.py          # Simulated load to Azure SQL + Delta Lake
├── utils/
│   ├── __init__.py
│   └── helpers.py                  # Shared download/extract utility
├── data/
│   ├── 01_raw/                     # Downloaded files (date-logged)
│   ├── 02_processed/               # Cleaned files (date-logged)
│   └── 03_transformed/             # Analysis outputs (date-logged)
├── docs/
│   └── presentation.pptx
├── .env                            # API keys
├── .gitignore
├── requirements.txt
└── README.md
```

---

## How to Run

### Setup

```bash
git clone https://github.com/CYPHRN/data-engineer-assessment.git
cd data-engineer-assessment

python -m venv venv
source venv/bin/activate        # Linux/Mac
venv\Scripts\activate           # Windows

pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file in the project root:

```
NREL_API_KEY=your_api_key_here
AZURE_SQL_CONN=your_connection_string   # CAN BE EMPTY IF SIMULATED FLAG IS ACTIVE
DELTA_LAKE_CONN=your_delta_path         # CAN BE EMPTY IF SIMULATED FLAG IS ACTIVE
```

Get a free NREL API key at https://developer.nrel.gov/signup/

Connection strings are only needed if you set `SIMULATION = False` in Script 04

### Run the Pipeline

Run scripts in order from the project root:

```bash
python scripts/01_data_acquisition.py
python scripts/02_data_processing.py
python scripts/03_data_transformation.py
python scripts/04_data_loading.py
```

Each script logs progress to the terminal
Second run on the same day skips already downloaded files

---

## Pipeline Overview

### Script 01 - Data Acquisition

- Downloads NHTSA complaints flat file (TXT)
- Calls NREL API for alternative fuel station data (JSON)
- Downloads EPA fuel economy (CSV)
- Files saved in date-logged folders (`data/01_raw/YYYY-MM-DD/`)
- Validates HTTP responses, skips if already downloaded today

### Script 02 - Data Processing

- Reduces 209 total columns to 33 relevant ones
- NHTSA: applies manual headers (file has none), filters vehicles only, removes year=9999, handles bad rows
- EPA: fills missing vehicle type labels, validates critical fields
- AFDC: parses JSON structure, filters open stations, converts dates
- Cleaned data saved to `data/02_processed/`

### Script 03 - Data Transformation

- Produces 4 analytical datasets answering the research questions
- Groups complaints by cylinder count (with outlier removal)
- Tracks vehicle type distribution by year (Standard, EV, Hybrid, etc.)
- Counts fuel station openings by year and fuel type
- Aggregates complaints by fuel type
- Results saved to `data/03_transformed/`

### Script 04 - Data Loading (Simulated)

- Simulates loading transformed data into Azure SQL Database and Databricks Delta Lake
- Uses a `SIMULATION = True` flag, production commands in comments
- Azure SQL: SQLAlchemy with `to_sql()`
- Delta Lake: PySpark with Delta format writer
- Set `SIMULATION = False` with real credentials to execute

---

## Automation Strategy

In production, this pipeline would be orchestrated with Azure Data Factory:

- Schedule: Weekly (Sunday 02:00 UTC). NHTSA updates daily but EPA/AFDC update less frequently. Weekly balances data freshness with cost.
- Pipeline flow: Script 01 > 02 > 03 > 04 sequentially
- Failure handling: Each source is independent. If one fails, the pipeline continues with available data and logs the failure.
- Retry policy: 2 retries with 10-minute intervals per activity
- Monitoring: ADF pipeline alerts via email on failure

---

## Tech Stack

- Python 3.12
- Pandas, NumPy (data processing)
- Requests (HTTP downloads + API calls)
- SQLAlchemy (simulated database loading)
- PySpark (simulated Delta Lake loading)
