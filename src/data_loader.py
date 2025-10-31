import pandas as pd
import sqlite3
from src.database import DATABASE_NAME, create_tables


def load_movielens_data():
    # Reads MovieLens CSVs and populates the SQLite database.
    # Path to the data files
    movies_path = 'data/movies.csv'
    ratings_path = 'data/ratings.csv'

    # Ensure tables exist before loading data
    create_tables()

    try:
        movies_df = pd.read_csv(movies_path)
        ratings_df = pd.read_csv(ratings_path)
    except FileNotFoundError as e:
        print(f"Error: {e}. Make sure 'movies.csv' and 'ratings.csv' are in the 'data/' directory.")
        return

    conn = sqlite3.connect(DATABASE_NAME)

    # Load data into the database.
    # 'if_exists' ensures that re-running this script will overwrite old data.
    movies_df.to_sql('movies', conn, if_exists='replace', index=False)
    ratings_df.to_sql('ratings', conn, if_exists='replace', index=False)

    print("MovieLens data loaded into the database successfully.")
    conn.close()


if __name__ == '__main__':
    # Allows us to run this script directly to populate the database
    load_movielens_data()