from auth import DatabaseManager
import pandas as pd
from database import ExtendedDatabaseManager
from api import BaseExtractor

def init_dim_date():
    """
    Initializes and populates the 'dim_date' dimension table with a full date range
    from 2000-01-01 to 2030-12-31.

    This function generates a DataFrame with fields:
        - full_date: actual date
        - date_id: integer representation in YYYYMMDD format
        - year, month, day: extracted date parts

    The data is then inserted into the 'dim_date' table using the DatabaseManager.
    """
    db = DatabaseManager("Movies.db")
    
    row_count = db.conn.sql("SELECT COUNT(*) AS count from dim_date").fetchone()[0]
    if row_count > 0:
        print("dim_date already populated.Skipping.")
        return
    # creating range of dates
    date_range = pd.date_range(start="2000-01-01", end="2030-12-31", freq="D")
    dim_date_df = pd.DataFrame({
        "full_date": date_range,
        "date_id": date_range.strftime("%Y%m%d").astype(int),
        "year": date_range.year,
        "month": date_range.month,
        "day": date_range.day
    })

    # inserting into dim_date from dataframe
    db.insert_from_df("dim_date", dim_date_df)

def load_to_staging_from_api():
    """
    Extracts movie data using the BaseExtractor, flattens the JSON structure,
    and loads the resulting data into the 'stg_Movies' staging table in the database.

    This function is responsible for populating the staging layer with raw movie data
    fetched from an external API or local test JSON.
    """
    db = DatabaseManager("Movies.db")
    extractor = BaseExtractor()
    results = extractor.fetch_data()
    df = pd.DataFrame(results)
    db.insert_from_df("stg_Movies",df)
    

def main():

    """
    Main ETL pipeline function that:
        - Creates all staging, dimension, and fact tables
        - Loads revenue data from CSV into staging
        - Loads date dimension table
        - Extracts and loads movie data into staging
        - Populates dimension and fact tables from staging data

    It serves as the entry point for building the movie data warehouse from scratch.
    """
    db = ExtendedDatabaseManager()
    db.create_staging_tables()
    db.create_dim_tables()
    db.create_fact_tables()

    db.load_csv_to_table("stg_Revenues", "revenues_per_day.csv")
    load_to_staging_from_api()
    init_dim_date()

    db.insert_to_dim_distrubtion()
    db.insert_to_dim_movie()
    db.insert_to_fact_revenue()

    db.insert_to_dim_genre()
    db.insert_to_dim_director()
    db.insert_to_dim_writer()
    db.insert_to_dim_actor()

    #bridge tables
    db.insert_to_bridge_movie_genre()
    db.insert_to_bridge_movie_director()
    db.insert_to_bridge_movie_writer()
    db.insert_to_bridge_movie_actor()


if __name__ == "__main__":
    main()