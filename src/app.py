from flask import Flask, render_template, request, session, redirect, url_for, flash
from surprise import Reader, Dataset, KNNWithMeans
from src.recommender import SimpleRecommender
from src.database import get_db_connection
import pandas as pd
import time



# Create the Flask application
app = Flask(__name__, template_folder='../templates', static_folder='../static')
app.secret_key = 'a-super-secret-key-that-you-should-change'

# Initialize the recommender once when the app starts
print("Initializing recommender... this may take a moment.")
recommender = SimpleRecommender(k=30)
print("Recommender initialized successfully.")


@app.route('/', methods=['GET', 'POST'])
def home():
    # Handles the home page and user 'login' by ID.
    if request.method == 'POST':
        try:
            user_id = int(request.form.get('user_id'))
            conn = get_db_connection()
            user_exists = conn.execute('SELECT 1 FROM ratings WHERE userId = ? LIMIT 1', (user_id,)).fetchone()
            conn.close()

            if user_exists:
                session['userId'] = user_id
                flash(f"Welcome back, User {user_id}! Your rating history has been loaded.", "success")
                return redirect(url_for('browse_movies'))
            else:
                flash(f"User ID {user_id} not found. Please try again or start as a new user.", "error")
        except (ValueError, TypeError):
            flash("Please enter a valid number for the User ID.", "error")
        return redirect(url_for('home'))

    # For a GET request, just display the login page
    return render_template('index.html')


@app.route('/movies')
def browse_movies():
    # Displays movies and assigns a new userId if one doesn't exist.
    if 'userId' not in session:
        conn = get_db_connection()
        max_user_id = conn.execute('SELECT MAX(userId) FROM ratings').fetchone()[0]
        session['userId'] = (max_user_id or 0) + 1
        conn.close()
        flash(f"Welcome! You have been assigned temporary User ID: {session['userId']}", "success")

    conn = get_db_connection()
    movies_df = pd.read_sql_query("SELECT movieId, title FROM movies LIMIT 100", conn)
    conn.close()
    movie_list = movies_df.to_dict('records')
    return render_template('movies.html', movies=movie_list)


@app.route('/add_rating', methods=['POST'])
def add_rating():
    # Saves a user's movie rating to the database.
    if 'userId' not in session:
        return redirect(url_for('browse_movies'))
    try:
        user_id = session['userId']
        movie_id = int(request.form['movieId'])
        rating = float(request.form['rating'])
        timestamp = int(time.time())

        # Insert or update the rating in the database
        conn = get_db_connection()
        conn.execute(
            'INSERT OR REPLACE INTO ratings (userId, movieId, rating, timestamp) VALUES (?, ?, ?, ?)',
            (user_id, movie_id, rating, timestamp)
        )
        conn.commit()
        conn.close()
        flash(f"Your rating of {rating} ★ has been saved to the database!", "success")
    except (ValueError, KeyError):
        flash("Invalid rating submission.", "error")
    return redirect(url_for('browse_movies'))


@app.route('/recommend')
def recommend():
    # Generates recommendations based on the user's saved ratings.
    if 'userId' not in session:
        flash("Please rate some movies first!", "error")
        return redirect(url_for('browse_movies'))

    user_id = session['userId']
    conn = get_db_connection()
    user_ratings_df = pd.read_sql_query(f"SELECT userId, movieId, rating FROM ratings WHERE userId = {user_id}", conn)

    if len(user_ratings_df) < 3:
        flash("You need to rate at least 3 movies to get recommendations.", "error")
        conn.close()
        return redirect(url_for('browse_movies'))

    # Load all ratings, including the new user's, for model training
    all_ratings_df = pd.read_sql_query("SELECT userId, movieId, rating FROM ratings", conn)
    conn.close()

    # Train a temporary model on the latest data
    reader = Reader(rating_scale=(0.5, 5.0))
    data = Dataset.load_from_df(all_ratings_df, reader)
    trainset = data.build_full_trainset()
    sim_options = {'name': 'cosine', 'user_based': True}
    temp_model = KNNWithMeans(k=30, sim_options=sim_options)
    temp_model.fit(trainset)

    # Generate predictions
    rated_movie_ids = set(user_ratings_df['movieId'])
    all_movie_ids = set(all_ratings_df['movieId'].unique())
    movies_to_predict = list(all_movie_ids - rated_movie_ids)
    predictions = [temp_model.predict(user_id, mid) for mid in movies_to_predict]
    predictions.sort(key=lambda x: x.est, reverse=True)
    top_predictions = predictions[:10]

    # Fetch movie titles for the top predictions
    recommendations = []
    if top_predictions:
        movie_ids = [pred.iid for pred in top_predictions]
        conn = get_db_connection()
        query = f"SELECT movieId, title FROM movies WHERE movieId IN {tuple(movie_ids)}"
        movies_df = pd.read_sql_query(query, conn)
        conn.close()
        movie_titles = movies_df.set_index('movieId')['title'].to_dict()

        for pred in top_predictions:
            recommendations.append({
                'title': movie_titles.get(pred.iid, 'Unknown Title'),
                'predicted_rating': round(pred.est, 2)
            })

    return render_template('recommend.html', recommendations=recommendations)


@app.route('/my-ratings')
def my_ratings():
    # Displays a list of all movies rated by the current user.
    if 'userId' not in session:
        flash("You haven't rated any movies yet.", "error")
        return redirect(url_for('browse_movies'))

    user_id = session['userId']
    conn = get_db_connection()
    # Join ratings with movies to get titles
    query = """
        SELECT m.title, r.rating
        FROM ratings r JOIN movies m ON r.movieId = m.movieId
        WHERE r.userId = ? ORDER BY r.timestamp DESC
    """
    my_ratings_df = pd.read_sql_query(query, conn, params=(user_id,))
    conn.close()
    my_ratings_list = my_ratings_df.to_dict('records')

    return render_template('my_ratings.html', ratings=my_ratings_list)


if __name__ == '__main__':
    app.run(debug=True)