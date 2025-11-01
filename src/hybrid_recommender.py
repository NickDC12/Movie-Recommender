import pandas as pd
import numpy as np
from surprise import Dataset, Reader, KNNWithMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from src.database import get_db_connection


class HybridRecommender:

    #Collaborative and Content-Based Filtering (genre)

    def __init__(self, k=30, collaborative_weight=0.7, content_weight=0.3):

        self.collaborative_weight = collaborative_weight
        self.content_weight = content_weight
        self.model = None
        self.all_movie_ids = None
        self.trainset = None
        self.movies_df = None
        self.content_similarity_matrix = None
        self.movie_id_to_index = None

        self._load_and_train(k=k)

    def _load_and_train(self, k: int):
        #Loads and train data for both collaborative and content-based models.
        print("Loading data and training hybrid model...")

        # Load data from the database
        conn = get_db_connection()
        ratings_df = pd.read_sql_query("SELECT userId, movieId, rating FROM ratings", conn)
        self.movies_df = pd.read_sql_query("SELECT movieId, title, genres FROM movies", conn)
        conn.close()

        self.all_movie_ids = set(self.movies_df['movieId'].unique())

        # Create movie ID to index mapping for content-based filtering
        self.movie_id_to_index = {movie_id: idx for idx, movie_id in enumerate(self.movies_df['movieId'])}

        # Train Collaborative Filtering Model
        self._train_collaborative_model(ratings_df, k)

        # building Content-Based Similarity Matrix
        self._build_content_similarity()

        print("Hybrid model training complete.")

    def _train_collaborative_model(self, ratings_df: pd.DataFrame, k: int):
        # Train KNN collab. filtering model
        print("Training collaborative filtering model...")

        reader = Reader(rating_scale=(0.5, 5.0))
        data = Dataset.load_from_df(ratings_df[['userId', 'movieId', 'rating']], reader)
        self.trainset = data.build_full_trainset()

        sim_options = {
            'name': 'cosine',
            'user_based': True,
            'min_support': 5
        }
        self.model = KNNWithMeans(k=k, sim_options=sim_options, verbose=False)
        self.model.fit(self.trainset)

        print("Collaborative filtering model trained.")

    def _build_content_similarity(self):
        # Content-based similarity matrix using different categories (genres for now. perhaps cast and keywords later??)
        print("Building content-based similarity matrix...")


        self.movies_df['content_features'] = self.movies_df['genres'].fillna('')

      # do the same for other categories (WIP)
        # self.movies_df['content_features'] = (
        #     self.movies_df['genres'].fillna('') + ' ' +
        #     self.movies_df['cast'].fillna('') + ' ' +
        #     self.movies_df['keywords'].fillna('')
        # )

        # Use TF-IDF to vectorize content features
        tfidf = TfidfVectorizer(
            token_pattern=r'[A-Za-z0-9]+',  # Handle pipe-separated genres
            lowercase=True,
            stop_words='english'
        )

        tfidf_matrix = tfidf.fit_transform(self.movies_df['content_features'])

        # Calculate cosine similarity matrix
        self.content_similarity_matrix = cosine_similarity(tfidf_matrix, tfidf_matrix)

        print("Content-based similarity matrix built.")

    def _get_collaborative_score(self, user_id: int, movie_id: int) -> float:
        # Get collaborative filtering prediction score for a movie.
        try:
            pred = self.model.predict(uid=user_id, iid=movie_id)
            return pred.est
        except:
            # Return neutral score if prediction failed
            return 3.0

    def _get_content_score(self, user_id: int, movie_id: int, rated_movie_ids: set,
                           user_ratings_cache: pd.DataFrame = None) -> float:

        if movie_id not in self.movie_id_to_index:
            return 0.0

        movie_idx = self.movie_id_to_index[movie_id]

        # Use cached ratings if provided, otherwise query database
        if user_ratings_cache is None:
            conn = get_db_connection()
            user_ratings = pd.read_sql_query(
                f"SELECT movieId, rating FROM ratings WHERE userId = {user_id}",
                conn
            )
            conn.close()
        else:
            user_ratings = user_ratings_cache

        if len(user_ratings) == 0:
            return 0.0

        # Calculate weighted similarity based on user's ratings
        similarity_scores = []

        for _, row in user_ratings.iterrows():
            rated_movie_id = row['movieId']
            rating = row['rating']

            if rated_movie_id in self.movie_id_to_index:
                rated_movie_idx = self.movie_id_to_index[rated_movie_id]
                similarity = self.content_similarity_matrix[movie_idx][rated_movie_idx]

                # Weight similarity by the user's rating (higher rated movies have more influence)
                weighted_similarity = similarity * (rating / 5.0)
                similarity_scores.append(weighted_similarity)

        # Return average weighted similarity
        return np.mean(similarity_scores) if similarity_scores else 0.0

    def get_recommendations(self, user_id: int, n: int = 10):

        try:
            # Get movies the user has already rated
            inner_user_id = self.trainset.to_inner_uid(user_id)
            rated_movies_inner_ids = self.trainset.ur[inner_user_id]
            rated_movie_ids = {self.trainset.to_raw_iid(inner_id) for inner_id, _ in rated_movies_inner_ids}
        except ValueError:
            # New user - only use content-based filtering
            print(f"New user {user_id}, using content-based filtering only.")
            rated_movie_ids = set()

            # Get any ratings this user might have from database
            conn = get_db_connection()
            user_ratings = pd.read_sql_query(
                f"SELECT movieId FROM ratings WHERE userId = {user_id}",
                conn
            )
            conn.close()

            if len(user_ratings) > 0:
                rated_movie_ids = set(user_ratings['movieId'])

        # Filter out already-rated movies
        movies_to_predict = self.all_movie_ids - rated_movie_ids

        # Limit predictions for faster responses
        if len(movies_to_predict) > 10000:
            print(f"Limiting predictions to 10000 movies for speed")
            movies_to_predict = list(movies_to_predict)[:10000]

        # Cache user ratings once to avoid querying for every movie
        conn = get_db_connection()
        user_ratings_cache = pd.read_sql_query(
            f"SELECT movieId, rating FROM ratings WHERE userId = {user_id}",
            conn
        )
        conn.close()

        # Generate hybrid scores
        predictions = []
        for movie_id in movies_to_predict:
            # Get collaborative filtering score
            collab_score = self._get_collaborative_score(user_id, movie_id)

            # Get content-based score (normalized to 0-5 scale) - pass cached ratings
            content_score = self._get_content_score(user_id, movie_id, rated_movie_ids, user_ratings_cache) * 5.0

            # Compute hybrid score
            hybrid_score = (
                    self.collaborative_weight * collab_score +
                    self.content_weight * content_score
            )

            predictions.append((movie_id, hybrid_score, collab_score, content_score))

        # Sort by hybrid score
        predictions.sort(key=lambda x: x[1], reverse=True)

        return predictions[:n]

    def get_similar_movies(self, movie_id: int, n: int = 10):

        if movie_id not in self.movie_id_to_index:
            print(f"Movie ID {movie_id} not found.")
            return []

        movie_idx = self.movie_id_to_index[movie_id]

        # Get similarity scores for this movie
        similarity_scores = self.content_similarity_matrix[movie_idx]

        # Create list of (movie_id, similarity)
        similar_movies = [
            (self.movies_df.iloc[idx]['movieId'], similarity_scores[idx])
            for idx in range(len(similarity_scores))
            if idx != movie_idx  # Exclude the movie itself
        ]

        # Sort by similarity
        similar_movies.sort(key=lambda x: x[1], reverse=True)

        return similar_movies[:n]

    # Explains why a movie was chosen.
    def explain_recommendation(self, user_id: int, movie_id: int) -> dict:

        collab_score = self._get_collaborative_score(user_id, movie_id)

        try:
            inner_user_id = self.trainset.to_inner_uid(user_id)
            rated_movies_inner_ids = self.trainset.ur[inner_user_id]
            rated_movie_ids = {self.trainset.to_raw_iid(inner_id) for inner_id, _ in rated_movies_inner_ids}
        except ValueError:
            rated_movie_ids = set()

        content_score = self._get_content_score(user_id, movie_id, rated_movie_ids) * 5.0
        hybrid_score = (
                self.collaborative_weight * collab_score +
                self.content_weight * content_score
        )

        # Get movie details
        movie_info = self.movies_df[self.movies_df['movieId'] == movie_id].iloc[0]

        return {
            'movie_id': movie_id,
            'title': movie_info['title'],
            'genres': movie_info['genres'],
            'hybrid_score': round(hybrid_score, 2),
            'collaborative_score': round(collab_score, 2),
            'content_score': round(content_score, 2),
            'collaborative_weight': self.collaborative_weight,
            'content_weight': self.content_weight
        }
