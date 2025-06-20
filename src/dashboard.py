import pandas as pd
import altair as alt
import streamlit as st
from auth import DatabaseManager

db = DatabaseManager("Movies.db")

# Preparing data and filters
genres = db.query_sql("SELECT DISTINCT genre_name FROM Dim_Genre ORDER BY genre_name")["genre_name"].tolist()
years = db.query_sql("SELECT DISTINCT year FROM dim_date ORDER BY year")["year"].tolist()

#  Streamlit UI filters
selected_genre = st.selectbox("Filter by Genre", ["All"] + genres)
selected_year = st.selectbox("Filter by Year", ["All"] + [str(y) for y in years])

rank_type = st.radio("Ranking Type", ["Top Movies", "Top Genres"])

# Dynamic where clause based on filter
conditions = []
if selected_genre != "All":
    conditions.append(f"g.genre_name = '{selected_genre}'")
if selected_year != "All":
    conditions.append(f"d.year = {selected_year}")

where_clause = " AND ".join(conditions)
if where_clause:
    where_clause = "WHERE " + where_clause

# SQL query depending on selected ranking type
if rank_type == "Top Movies":
    query = f"""
    SELECT 
        m.title,
        SUM(fr.revenue) AS total_revenue
    FROM fact_revenue fr
    JOIN dim_movies m ON fr.movie_id = m.movie_id
    JOIN Bridge_Movie_Genre bmg ON m.movie_id = bmg.movie_id
    JOIN Dim_Genre g ON bmg.genre_id = g.genre_id
    JOIN dim_date d ON fr.date_id = d.date_id
    {where_clause}
    GROUP BY m.title
    ORDER BY total_revenue DESC
    LIMIT 10;
    """
else:
    query = f"""
    SELECT 
        g.genre_name,
        SUM(fr.revenue) AS total_revenue
    FROM fact_revenue fr
    JOIN dim_movies m ON fr.movie_id = m.movie_id
    JOIN Bridge_Movie_Genre bmg ON m.movie_id = bmg.movie_id
    JOIN Dim_Genre g ON bmg.genre_id = g.genre_id
    JOIN dim_date d ON fr.date_id = d.date_id
    {where_clause}
    GROUP BY g.genre_name
    ORDER BY total_revenue DESC
    LIMIT 10;
    """

df = db.query_sql(query)

# # # # # # #
# DASHBOARD #
# # # # # # #
st.title("Movie Revenue Rankings")

st.dataframe(df)

label = "title" if rank_type == "Top Movies" else "genre_name"

chart = alt.Chart(df).mark_bar().encode(
    x=alt.X(f'{label}:N', sort='-y', title="Movie" if label == "title" else "Genre"),
    y=alt.Y('total_revenue:Q', title="Total Revenue"),
    tooltip=[label, 'total_revenue']
).properties(
    width=700,
    height=400,
    title=f'{rank_type} by Revenue'
)

st.altair_chart(chart, use_container_width=True)

db.close_db()
