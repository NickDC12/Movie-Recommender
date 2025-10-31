import pandas as pd
from surprise import Dataset, Reader, KNNWithMeans
from src.database import get_db_connection


class SimpleRecommender:
    # A KNN-based collaborative filtering recommender system.

    def __init__(self, k=30):
        # Initializes the recommender by loading data and training the model.
        self.model = None
        self.all_movie_ids = None
        self.trainset = None
        self._load_and_train(k=k)

    def _load_and_train(self, k: int):
        # Loads rating data from the database and trains the KNN model.
        print("Loading data and training model...")

        # Load data from the database
        conn = get_db_connection()
        ratings_df = pd.read_sql_query("SELECT userId, movieId, rating FROM ratings", conn)
        movies_df = pd.read_sql_query("SELECT movieId FROM movies", conn)
        conn.close()

        self.all_movie_ids = set(movies_df['movieId'].unique())

        # Configure Surprise reader and load the data
        reader = Reader(rating_scale=(0.5, 5.0))
        data = Dataset.load_from_df(ratings_df[['userId', 'movieId', 'rating']], reader)
        self.trainset = data.build_full_trainset()

        # Configure the KNN algorithm
        sim_options = {
            'name': 'cosine',
            'user_based': True,
            'min_support': 5  # Min common ratings between users
        }
        self.model = KNNWithMeans(k=k, sim_options=sim_options, verbose=True)
        self.model.fit(self.trainset)

        print("Model training complete.")

    def get_recommendations(self, user_id: int, n: int = 10):
        # Generates movie recommendations for a given user.
        try:
            # Convert raw user ID to Surprise's inner ID
            inner_user_id = self.trainset.to_inner_uid(user_id)
        except ValueError:
            print(f"User with ID {user_id} not found in the dataset.")
            return []

        # Get movies the user has already rated
        rated_movies_inner_ids = self.trainset.ur[inner_user_id]
        rated_movie_ids = {self.trainset.to_raw_iid(inner_id) for inner_id, _ in rated_movies_inner_ids}

        # Filter out already-rated movies to get a list of movies to predict
        movies_to_predict = self.all_movie_ids - rated_movie_ids

        # Predict ratings for all unrated movies
        predictions = []
        for movie_id in movies_to_predict:
            pred = self.model.predict(uid=user_id, iid=movie_id)
            predictions.append((movie_id, pred.est))

        # Sort predictions by estimated rating in desc. order
        predictions.sort(key=lambda x: x[1], reverse=True)

        return predictions[:n]