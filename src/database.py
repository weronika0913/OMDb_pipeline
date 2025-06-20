"""
Module extending DatabaseManager to include methods for creating and inserting staging,
dimension, and fact tables specific to the movie database schema.
"""

from auth import DatabaseManager


# # # # # # # # # # #
#Creating warehouse #
# # # # # # # # # # # 

class ExtendedDatabaseManager(DatabaseManager):
    """
    ExtendedDatabaseManager inherits from DatabaseManager and provides
    additional methods to create schema tables for staging, dimension,
    and fact layers in a data warehouse for movie-related data.
    """

    def create_staging_tables(self):
        """
        Creates staging tables in the database to temporarily hold raw
        movie and revenue data before transformation.
        """
        sql = """
        CREATE TABLE IF NOT EXISTS stg_Revenues (
            id UUID,
            date DATE,
            title VARCHAR,
            revenue BIGINT,
            theaters INT,
            distributor VARCHAR
        );
        CREATE TABLE IF NOT EXISTS stg_Movies (
            Title VARCHAR,
            Year VARCHAR,
            Rated VARCHAR,
            Released VARCHAR,
            Runtime VARCHAR,
            Genre VARCHAR,
            Director VARCHAR,
            Writer VARCHAR,
            Actors VARCHAR,
            Plot VARCHAR,
            Language VARCHAR,
            Country VARCHAR,
            Awards VARCHAR,
            Poster VARCHAR,
            Ratings_0_Source VARCHAR,
            Ratings_0_Value VARCHAR,
            Ratings_1_Source VARCHAR,
            Ratings_1_Value VARCHAR,
            Ratings_2_Source VARCHAR,
            Ratings_2_Value VARCHAR,
            Metascore VARCHAR,
            imdbRating VARCHAR,
            imdbVotes VARCHAR,
            imdbID VARCHAR,
            Type VARCHAR,
            DVD VARCHAR,
            BoxOffice VARCHAR,
            Production VARCHAR,
            Website VARCHAR,
            Response VARCHAR
        );
        """
        self.execute_sql(sql, "Successfully created staging tables")

    def create_dim_tables(self):
        sql = """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id INT PRIMARY KEY,
            full_date DATE NOT NULL,
            year INT NOT NULL,
            month INT NOT NULL,
            day INT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS dim_movies (
            movie_id INT PRIMARY KEY,
            title VARCHAR NOT NULL,
            year VARCHAR,
            rated VARCHAR,
            released VARCHAR,
            runtime VARCHAR
        );

        CREATE TABLE IF NOT EXISTS dim_distribution (
            distribution_id INT PRIMARY KEY,
            name VARCHAR NOT NULL
        );

        CREATE TABLE IF NOT EXISTS Dim_Genre (
            genre_id INT PRIMARY KEY,
            genre_name VARCHAR NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS Dim_Director (
            director_id INT PRIMARY KEY,
            director_name VARCHAR NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS Dim_Writer (
            writer_id INT PRIMARY KEY,
            writer_name VARCHAR NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS Dim_Actor (
            actor_id INT PRIMARY KEY,
            actor_name VARCHAR NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS Bridge_Movie_Genre (
            movie_id INT NOT NULL,
            genre_id INT NOT NULL,
            PRIMARY KEY (movie_id, genre_id),
            FOREIGN KEY (movie_id) REFERENCES dim_movies(movie_id),
            FOREIGN KEY (genre_id) REFERENCES Dim_Genre(genre_id)
        );

        CREATE TABLE IF NOT EXISTS Bridge_Movie_Director (
            movie_id INT NOT NULL,
            director_id INT NOT NULL,
            PRIMARY KEY (movie_id, director_id),
            FOREIGN KEY (movie_id) REFERENCES dim_movies(movie_id),
            FOREIGN KEY (director_id) REFERENCES Dim_Director(director_id)
        );

        CREATE TABLE IF NOT EXISTS Bridge_Movie_Writer (
            movie_id INT NOT NULL,
            writer_id INT NOT NULL,
            PRIMARY KEY (movie_id, writer_id),
            FOREIGN KEY (movie_id) REFERENCES dim_movies(movie_id),
            FOREIGN KEY (writer_id) REFERENCES Dim_Writer(writer_id)
        );

        CREATE TABLE IF NOT EXISTS Bridge_Movie_Actor (
            movie_id INT NOT NULL,
            actor_id INT NOT NULL,
            PRIMARY KEY (movie_id, actor_id),
            FOREIGN KEY (movie_id) REFERENCES dim_movies(movie_id),
            FOREIGN KEY (actor_id) REFERENCES Dim_Actor(actor_id)
        );
        """
        self.execute_sql(sql, "Successfully created dimension tables")

    def create_fact_tables(self):
        """
        Creates fact tables that store measurable, quantitative data such as
        movie revenue linked to dimension tables via foreign keys.
        """
        sql = """
        CREATE TABLE IF NOT EXISTS fact_revenue (
            revenue_id UUID PRIMARY KEY,
            movie_id INT NOT NULL,
            date_id INT NOT NULL,
            distribution_id INT NOT NULL,
            revenue BIGINT NOT NULL,
            theaters INT,
            FOREIGN KEY (movie_id) REFERENCES dim_movies(movie_id),
            FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
            FOREIGN KEY (distribution_id) REFERENCES dim_distribution(distribution_id)
        );
        """
        self.execute_sql(sql, "Successfully created fact tables")

# # # # # # # # # # #
# Load to warehouse #
# # # # # # # # # # # 

    def insert_to_dim_distrubtion(self):
        sql = """
        INSERT INTO dim_distribution
        SELECT 
            (SELECT COALESCE(MAX(distribution_id), 0) FROM dim_distribution) + ROW_NUMBER() OVER () AS distribution_id,
            distributor
        FROM (
            SELECT distributor
            FROM stg_Revenues
            EXCEPT
            SELECT name FROM dim_distribution
        ) AS new_distributors;
        """
        self.execute_sql(sql,"Sucessfully loaded into distribution dimension")
    
    def insert_to_dim_movie(self):
        sql = """
        INSERT INTO dim_movies
        SELECT 
            (SELECT COALESCE(MAX(movie_id), 0) FROM dim_movies) + ROW_NUMBER() OVER (ORDER BY Title) AS movie_id,
            replace(Title, '.', '') as Title, Year, Rated, Released, Runtime
        FROM (
            SELECT DISTINCT replace(Title, '.', '') as Title, Year, Rated, Released, Runtime
            FROM stg_Movies
            EXCEPT
            SELECT Title, Year, Rated, Released, Runtime
            FROM dim_movies
        ) AS new_movies;
        """
        self.execute_sql(sql, "Successfully loaded into movie dimension")

    def insert_to_fact_revenue(self):
        sql = """
        INSERT INTO fact_revenue (
            revenue_id, movie_id, date_id, distribution_id, revenue, theaters
        )
        SELECT 
            sr.id AS revenue_id,
            dt.movie_id,
            dd.date_id,
            dist.distribution_id,
            sr.revenue,
            sr.theaters
        FROM stg_Revenues sr
        JOIN dim_movies dt ON sr.title = dt.title
        JOIN dim_date dd ON sr.date = dd.full_date
        JOIN dim_distribution dist ON sr.distributor = dist.name
        WHERE NOT EXISTS (
            SELECT 1 FROM fact_revenue fr
            WHERE fr.revenue_id = sr.id
        );
        """
        self.execute_sql(sql, "Successfully loaded into revenue fact table")


    def insert_to_dim_genre(self):
        sql = """
        INSERT INTO Dim_Genre (genre_id, genre_name)
        SELECT 
            (SELECT COALESCE(MAX(genre_id), 0) FROM Dim_Genre) + ROW_NUMBER() OVER (ORDER BY genre_name) AS genre_id,
            TRIM(REPLACE(genre_name, '.', '')) AS genre_name
        FROM (
            SELECT DISTINCT TRIM(REPLACE(genre_name, '.', '')) AS genre_name
            FROM stg_Movies,
            UNNEST(split(Genre, ',')) AS genre(genre_name)
            EXCEPT
            SELECT genre_name FROM Dim_Genre
        ) AS new_genres;
        """
        self.execute_sql(sql, "Successfully loaded into genre dimension")

    def insert_to_bridge_movie_genre(self):
        sql = """
        WITH movie_genre_mapping AS (
            SELECT
                m.movie_id,
                TRIM(REPLACE(genre_name, '.', '')) AS genre_name
            FROM stg_Movies s
            JOIN dim_movies m ON LOWER(TRIM(REPLACE(m.title, '.', ''))) = LOWER(TRIM(REPLACE(s.Title, '.', '')))
            CROSS JOIN UNNEST(split(s.Genre, ',')) AS genre(genre_name)
        )
        INSERT INTO Bridge_Movie_Genre (movie_id, genre_id)
        SELECT
            mgm.movie_id,
            g.genre_id
        FROM movie_genre_mapping mgm
        JOIN dim_genre g ON TRIM(g.genre_name) = mgm.genre_name
        WHERE NOT EXISTS (
            SELECT 1 FROM Bridge_Movie_Genre b
            WHERE b.movie_id = mgm.movie_id
            AND b.genre_id = g.genre_id
        );
        """
        self.execute_sql(sql, "Successfully loaded into bridge_movie_genre")


    def insert_to_dim_director(self):
        sql = """
        INSERT INTO Dim_Director (director_id, director_name)
        SELECT 
            (SELECT COALESCE(MAX(director_id), 0) FROM Dim_Director) + ROW_NUMBER() OVER (ORDER BY director_name) AS director_id,
            director_name
        FROM (
            SELECT DISTINCT TRIM(director_name) AS director_name
            FROM stg_Movies,
            UNNEST(split(Director, ',')) AS director(director_name)
            EXCEPT
            SELECT director_name FROM Dim_Director
        ) AS new_directors;
        """
        self.execute_sql(sql, "Successfully loaded into director dimension")

    def insert_to_dim_writer(self):
        sql = """
        INSERT INTO Dim_Writer (writer_id, writer_name)
        SELECT 
            (SELECT COALESCE(MAX(writer_id), 0) FROM Dim_Writer) + ROW_NUMBER() OVER (ORDER BY writer_name) AS writer_id,
            writer_name
        FROM (
            SELECT DISTINCT TRIM(writer_name) AS writer_name
            FROM stg_Movies,
            UNNEST(split(Writer, ',')) AS writer(writer_name)
            EXCEPT
            SELECT writer_name FROM Dim_Writer
        ) AS new_writers;
        """
        self.execute_sql(sql, "Successfully loaded into writer dimension")


    def insert_to_dim_actor(self):
        sql = """
        INSERT INTO Dim_Actor (actor_id, actor_name)
        SELECT 
            (SELECT COALESCE(MAX(actor_id), 0) FROM Dim_Actor) + ROW_NUMBER() OVER (ORDER BY actor_name) AS actor_id,
            actor_name
        FROM (
            SELECT DISTINCT TRIM(actor_name) AS actor_name
            FROM stg_Movies,
            UNNEST(split(Actors, ',')) AS actor(actor_name)
            EXCEPT
            SELECT actor_name FROM Dim_Actor
        ) AS new_actors;
        """
        self.execute_sql(sql, "Successfully loaded into actor dimension")


    def insert_to_bridge_movie_director(self):
        sql = """
        WITH movie_director_mapping AS (
            SELECT
                m.movie_id,
                TRIM(REPLACE(director_name, '.', '')) AS director_name
            FROM stg_Movies s
            JOIN dim_movies m ON LOWER(TRIM(REPLACE(m.title, '.', ''))) = LOWER(TRIM(REPLACE(s.Title, '.', '')))
            CROSS JOIN UNNEST(split(s.Director, ',')) AS director(director_name)
        )
        INSERT INTO Bridge_Movie_Director (movie_id, director_id)
        SELECT
            mdm.movie_id,
            d.director_id
        FROM movie_director_mapping mdm
        JOIN Dim_Director d ON TRIM(REPLACE(d.director_name, '.', '')) = mdm.director_name
        WHERE NOT EXISTS (
            SELECT 1 FROM Bridge_Movie_Director b
            WHERE b.movie_id = mdm.movie_id
            AND b.director_id = d.director_id
        );
        """
        self.execute_sql(sql, "Successfully loaded into bridge_movie_director")

    def insert_to_bridge_movie_writer(self):
        sql = """
        WITH movie_writer_mapping AS (
            SELECT
                m.movie_id,
                TRIM(REPLACE(writer_name, '.', '')) AS writer_name
            FROM stg_Movies s
            JOIN dim_movies m ON LOWER(TRIM(REPLACE(m.title, '.', ''))) = LOWER(TRIM(REPLACE(s.Title, '.', '')))
            CROSS JOIN UNNEST(split(s.Writer, ',')) AS writer(writer_name)
        )
        INSERT INTO Bridge_Movie_Writer (movie_id, writer_id)
        SELECT
            mwm.movie_id,
            w.writer_id
        FROM movie_writer_mapping mwm
        JOIN Dim_Writer w ON TRIM(REPLACE(w.writer_name, '.', '')) = mwm.writer_name
        WHERE NOT EXISTS (
            SELECT 1 FROM Bridge_Movie_Writer b
            WHERE b.movie_id = mwm.movie_id
            AND b.writer_id = w.writer_id
        );
        """
        self.execute_sql(sql, "Successfully loaded into bridge_movie_writer")

    def insert_to_bridge_movie_actor(self):
        sql = """
        WITH movie_actor_mapping AS (
            SELECT
                m.movie_id,
                TRIM(REPLACE(actor_name, '.', '')) AS actor_name
            FROM stg_Movies s
            JOIN dim_movies m ON LOWER(TRIM(REPLACE(m.title, '.', ''))) = LOWER(TRIM(REPLACE(s.Title, '.', '')))
            CROSS JOIN UNNEST(split(s.Actors, ',')) AS actor(actor_name)
        )
        INSERT INTO Bridge_Movie_Actor (movie_id, actor_id)
        SELECT
            mam.movie_id,
            a.actor_id
        FROM movie_actor_mapping mam
        JOIN Dim_Actor a ON TRIM(REPLACE(a.actor_name, '.', '')) = mam.actor_name
        WHERE NOT EXISTS (
            SELECT 1 FROM Bridge_Movie_Actor b
            WHERE b.movie_id = mam.movie_id
            AND b.actor_id = a.actor_id
        );
        """
        self.execute_sql(sql, "Successfully loaded into bridge_movie_actor")

