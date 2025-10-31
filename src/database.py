import sqlite3
import pandas as pd

DATABASE_NAME = 'movielens.db'


def get_db_connection():
    # Establishes a connection to the SQLite database.
    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row  # Allows accessing columns by name
    return conn


def create_tables():
    # Creates the movies and ratings tables if they don't already exist.
    conn = get_db_connection()
    cursor = conn.cursor()

    # Define the schema for the movies table
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS movies
                   (
                       movieId INTEGER PRIMARY KEY,
                       title TEXT NOT NULL,
                       genres TEXT NOT NULL
                   );
                   ''')

    # Define the schema for the ratings table
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS ratings
                   (
                       userId INTEGER NOT NULL,
                       movieId INTEGER NOT NULL,
                       rating REAL NOT NULL,
                       timestamp INTEGER NOT NULL,
                       PRIMARY KEY (userId, movieId),
                       FOREIGN KEY (movieId) REFERENCES movies(movieId)
                       );
                   ''')
    conn.commit()
    conn.close()
    print("Tables created successfully.")