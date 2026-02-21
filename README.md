# NYC Yellow Taxi Dashboard

🚕 **Deployed Streamlit App:** https://nycyellowtaxidatapipelinevisualization-9i2bg8a5jhb3huazoffses.streamlit.app/

**NB: Due to memory issues, only 500k of the dataset was used for the app**

An interactive dashboard for exploring NYC Yellow Taxi trip data built with Streamlit and Plotly.

## Features

- Filter trips by date range, hour of day, and payment type
- Top 10 pickup zones
- Average fare by hour of day
- Trip distance distribution
- Payment type breakdown
- Trips by day of week and hour heatmap

## Prerequisites

- Python 3.8+

## Setup

1. **Clone the repository**

   ```bash
   git clone <your-repo-url>
   cd <your-repo-name>
   ```

2. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

3. **Run the app**
   ```bash
   streamlit run app.py
   ```

The app will automatically download the required data files on first launch:

- NYC Yellow Taxi trip data (January 2024) — saved to `data/raw/yellow_taxi.parquet`
- Taxi zone lookup table — saved to `data/raw/taxi_lookup.csv`

## Jupyter Notebook

`assignment1.ipynb` walks through the full data pipeline step by step:

1. **Data Ingestion** — downloading the parquet and CSV files
2. **Storage** — saving and loading data locally
3. **Cleaning** — removing nulls, filtering invalid trips and fares
4. **Transformation** — adding derived columns such as trip duration, speed, pickup hour, and day of week
5. **Analysis** — querying and aggregating data using Polars and DuckDB
6. **Visualization** — charts and plots exploring trip patterns, fares, distances, and payment types

To run the notebook:

```bash
jupyter notebook assignment1.ipynb
```

## Project Structure

```
.
├── app.py
├── requirements.txt
├── assignment1.ipynb
├── .gitignore
└── data/
    └── raw/
        ├── yellow_taxi.parquet   # auto-downloaded on first run
        └── taxi_lookup.csv       # auto-downloaded on first run
```
