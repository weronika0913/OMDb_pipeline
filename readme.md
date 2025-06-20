# Movie Data Warehouse

## Description

This project is a simple pipeline for building a movie data warehouse.

### What does it do?

- **Fetches movie data** from the OMDb API using an API token stored in a `.env` file.
- **Loads local CSV data** with daily revenues (`revenues_per_day.csv`) into a DuckDB database.
- **Creates and manages tables** for staging, dimensions (like `dim_date`, `dim_movie`, `dim_genre`), and facts (`fact_revenue`).
- **Processes and loads data** into the data warehouse tables, including movie details, dates, genres, directors, actors, and more.
- **Handles many-to-many relationships** via bridge tables.
- **Maintains idempotency** – all ETL steps can be safely rerun without duplicating data.

## Project Structure

- `auth.py` — manages API token and DuckDB connection.
- `database.py` — contains class for database operations.
- `api.py` — fetches movie data from the OMDb API and flattens nested JSON responses.
- `main.py` — main ETL pipeline script that runs the full process of creating tables and loading data.

## Setup Instructions

1. Install dependencies and activate the Poetry environment:

    ```bash
    poetry install
    poetry shell
    ```

2. Create a `.env` file in the project root with your OMDb API key:

    ```
    API_KEY=your_api_key_here
    ```

3. Place the `revenues_per_day.csv` file in the project folder.

4. Run the ETL pipeline:

    ```bash
    python main.py
    ```

## Streamlit Dashboard

A Streamlit dashboard is available to visualize the movie data warehouse.

### How to run the dashboard:

1. Run the dashboard script (e.g., `dashboard.py`):

    ```bash
    streamlit run dashboard.py
    ```

2. The dashboard will open in your browser to explore movie data and revenues.

---