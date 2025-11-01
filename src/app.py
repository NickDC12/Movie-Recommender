from flask import Flask, render_template, request, session, redirect, url_for, flash, jsonify
from surprise import Reader, Dataset, KNNWithMeans
from src.hybrid_recommender import HybridRecommender
from src.database import get_db_connection
import pandas as pd
import time

# Create the Flask application
app = Flask(__name__, template_folder='../templates', static_folder='../static')
app.secret_key = 'a-super-secret-key-that-you-should-change'

# Initialize the hybrid recommender once when the app starts
print("Initializing hybrid recommender... this may take a moment.")
recommender = HybridRecommender(k=30, collaborative_weight=0.7, content_weight=0.3)
print("Hybrid recommender initialized successfully.")


@app.route('/', methods=['GET', 'POST'])
def home():
    """Handles the home page and user 'login' by ID."""
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

    return render_template('index.html')


@app.route('/movies')
def browse_movies():
    """Displays movies and assigns a new userId if one doesn't exist."""
    if 'userId' not in session:
        conn = get_db_connection()
        max_user_id = conn.execute('SELECT MAX(userId) FROM ratings').fetchone()[0]
        session['userId'] = (max_user_id or 0) + 1
        conn.close()
        flash(f"Welcome! You have been assigned temporary User ID: {session['userId']}", "success")

    conn = get_db_connection()
    movies_df = pd.read_sql_query("SELECT movieId, title, genres FROM movies LIMIT 100", conn)
    conn.close()
    movie_list = movies_df.to_dict('records')
    return render_template('movies.html', movies=movie_list)


@app.route('/add_rating', methods=['POST'])
def add_rating():
    """Saves a user's movie rating to the database."""
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
        flash(f"Your rating of {rating} ⭐ has been saved to the database!", "success")
    except (ValueError, KeyError):
        flash("Invalid rating submission.", "error")
    return redirect(url_for('browse_movies'))


@app.route('/api/search')
def search_movies():
    """API endpoint for movie search autocomplete."""
    query = request.args.get('q', '').strip()

    if not query or len(query) < 2:
        return jsonify([])

    conn = get_db_connection()
    search_query = f"""
           SELECT movieId, title, genres 
           FROM movies 
           WHERE title LIKE ? 
           LIMIT 10
       """
    movies_df = pd.read_sql_query(search_query, conn, params=(f'%{query}%',))
    conn.close()

    results = movies_df.to_dict('records')
    return jsonify(results)

@app.route('/recommend')
def recommend():
    """Generates hybrid recommendations based on the user's saved ratings."""
    if 'userId' not in session:
        flash("Please rate some movies first!", "error")
        return redirect(url_for('browse_movies'))

    user_id = session['userId']
    conn = get_db_connection()
    user_ratings_df = pd.read_sql_query(
        f"SELECT userId, movieId, rating FROM ratings WHERE userId = {user_id}",
        conn
    )

    if len(user_ratings_df) < 3:
        flash("You need to rate at least 3 movies to get recommendations.", "error")
        conn.close()
        return redirect(url_for('browse_movies'))

    conn.close()

    # Use the hybrid recommender
    print(f"Generating hybrid recommendations for user {user_id}...")
    predictions = recommender.get_recommendations(user_id, n=10)

    # Fetch movie details
    recommendations = []
    if predictions:
        movie_ids = [pred[0] for pred in predictions]
        conn = get_db_connection()

        # Handle single movie ID case
        if len(movie_ids) == 1:
            query = f"SELECT movieId, title, genres FROM movies WHERE movieId = {movie_ids[0]}"
        else:
            query = f"SELECT movieId, title, genres FROM movies WHERE movieId IN {tuple(movie_ids)}"

        movies_df = pd.read_sql_query(query, conn)
        conn.close()
        movie_info = movies_df.set_index('movieId').to_dict('index')

        for pred in predictions:
            movie_id, hybrid_score, collab_score, content_score = pred
            if movie_id in movie_info:
                recommendations.append({
                    'movieId': movie_id,
                    'title': movie_info[movie_id]['title'],
                    'genres': movie_info[movie_id]['genres'],
                    'hybrid_score': round(hybrid_score, 2),
                    'collaborative_score': round(collab_score, 2),
                    'content_score': round(content_score, 2)
                })

    return render_template('recommend.html', recommendations=recommendations)


@app.route('/similar/<int:movie_id>')
def similar_movies(movie_id):
    """Finds movies similar to the given movie based on content features."""
    similar = recommender.get_similar_movies(movie_id, n=10)

    if not similar:
        flash(f"Movie ID {movie_id} not found.", "error")
        return redirect(url_for('browse_movies'))

    # Fetch movie details
    movie_ids = [sim[0] for sim in similar]
    conn = get_db_connection()

    # Get the original movie info
    original_movie = pd.read_sql_query(
        f"SELECT title, genres FROM movies WHERE movieId = {movie_id}",
        conn
    ).iloc[0]

    # Get similar movies info
    if len(movie_ids) == 1:
        query = f"SELECT movieId, title, genres FROM movies WHERE movieId = {movie_ids[0]}"
    else:
        query = f"SELECT movieId, title, genres FROM movies WHERE movieId IN {tuple(movie_ids)}"

    movies_df = pd.read_sql_query(query, conn)
    conn.close()

    movie_info = movies_df.set_index('movieId').to_dict('index')

    similar_list = []
    for sim in similar:
        mid, similarity = sim
        if mid in movie_info:
            similar_list.append({
                'title': movie_info[mid]['title'],
                'genres': movie_info[mid]['genres'],
                'similarity': round(similarity * 100, 1)  # Convert to percentage
            })

    return render_template('similar.html',
                           original_title=original_movie['title'],
                           original_genres=original_movie['genres'],
                           similar_movies=similar_list)


@app.route('/explain/<int:movie_id>')
def explain_recommendation(movie_id):
    """Provides an explanation for why a movie was recommended."""
    if 'userId' not in session:
        flash("Please log in first!", "error")
        return redirect(url_for('home'))

    user_id = session['userId']
    explanation = recommender.explain_recommendation(user_id, movie_id)

    return jsonify(explanation)


@app.route('/my-ratings')
def my_ratings():
    """Displays a list of all movies rated by the current user."""
    if 'userId' not in session:
        flash("You haven't rated any movies yet.", "error")
        return redirect(url_for('browse_movies'))

    user_id = session['userId']
    conn = get_db_connection()
    query = """
        SELECT m.title, m.genres, r.rating
        FROM ratings r JOIN movies m ON r.movieId = m.movieId
        WHERE r.userId = ? ORDER BY r.timestamp DESC
    """
    my_ratings_df = pd.read_sql_query(query, conn, params=(user_id,))
    conn.close()
    my_ratings_list = my_ratings_df.to_dict('records')

    return render_template('my_ratings.html', ratings=my_ratings_list)


if __name__ == '__main__':
    app.run(debug=True)