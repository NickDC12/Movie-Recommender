from flask import Flask, render_template, request, session, redirect, url_for, flash, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from sqlite3 import IntegrityError
from functools import wraps
from surprise import Reader, Dataset, KNNWithMeans
from src.hybrid_recommender import HybridRecommender
from src.database import get_db_connection
import pandas as pd
import time

app = Flask(__name__, template_folder='../templates', static_folder='../static')
app.secret_key = 'a-super-secret-key-that-you-should-change'

print("Initializing hybrid recommender... this may take a moment.")
recommender = HybridRecommender(k=30, collaborative_weight=0.7, content_weight=0.3)
print("Hybrid recommender initialized successfully.")


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'userId' not in session:
            flash('Please log in to access this page.', 'error')
            return redirect(url_for('home'))
        return f(*args, **kwargs)

    return decorated_function


def validate_username(username):
    if not username or len(username) < 3:
        return False, "Username must be at least 3 characters"
    if len(username) > 20:
        return False, "Username must be less than 20 characters"
    if not username.replace('_', '').replace('-', '').isalnum():
        return False, "Username can only contain letters, numbers, underscores, and hyphens"
    return True, None


def validate_password(password):
    if not password or len(password) < 6:
        return False, "Password must be at least 6 characters"
    return True, None


@app.route('/')
def home():
    # Landing page - redirects to guest profile selector
    return redirect(url_for('guest_profiles'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    # Login page for registered users"""
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')

        conn = get_db_connection()
        user = conn.execute(
            'SELECT * FROM users WHERE username = ? AND is_anonymous = 0',
            (username,)
        ).fetchone()
        conn.close()

        if user and check_password_hash(user['password'], password):
            session['userId'] = user['userId']
            session['username'] = user['username']
            session['is_anonymous'] = False
            session['is_guest'] = False
            flash(f"Welcome back, {username}!", "success")
            return redirect(url_for('browse_movies'))
        else:
            flash("Invalid username or password.", "error")
            return redirect(url_for('login'))

    return render_template('index.html')


@app.route('/guest-profiles')
def guest_profiles():
    # Show page to select a guest profile
    return render_template('guest_profiles.html')


@app.route('/api/guest-profiles')
def api_guest_profiles():
    #API endpoint to get demo profiles (optimized users with 10-15 ratings)
    conn = get_db_connection()

    profiles = conn.execute('''
        SELECT 
            u.userId,
            u.username,
            COUNT(r.rating) as rating_count,
            ROUND(AVG(r.rating), 1) as avg_rating,
            (
                SELECT m.genres 
                FROM ratings r2 
                JOIN movies m ON r2.movieId = m.movieId 
                WHERE r2.userId = u.userId 
                GROUP BY m.genres 
                ORDER BY COUNT(*) DESC 
                LIMIT 1
            ) as top_genre
        FROM users u
        JOIN ratings r ON u.userId = r.userId
        WHERE u.is_anonymous = 1
        GROUP BY u.userId
        ORDER BY u.userId DESC
    ''').fetchall()

    conn.close()

    profile_list = []
    for p in profiles:
        profile_list.append({
            'userId': p['userId'],
            'username': p['username'],
            'rating_count': p['rating_count'],
            'avg_rating': p['avg_rating'],
            'top_genre': p['top_genre'].split('|')[0] if p['top_genre'] else 'Various'
        })

    return jsonify(profile_list)


@app.route('/select-guest', methods=['POST'])
def select_guest():
    """Set session to browse as a guest user"""
    guest_user_id = request.form.get('guest_user_id')

    if not guest_user_id:
        flash("Invalid guest profile selection.", "error")
        return redirect(url_for('guest_profiles'))

    conn = get_db_connection()

    user = conn.execute(
        'SELECT * FROM users WHERE userId = ? AND is_anonymous = 1',
        (guest_user_id,)
    ).fetchone()

    conn.close()

    if user:
        session['userId'] = int(guest_user_id)
        session['username'] = user['username']
        session['is_guest'] = True
        session['is_anonymous'] = True
        flash(f"Browsing as {user['username']}!", "success")
        return redirect(url_for('browse_movies'))
    else:
        flash("Invalid guest profile.", "error")
        return redirect(url_for('guest_profiles'))


@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        password_confirm = request.form.get('password_confirm', '')

        valid_user, user_error = validate_username(username)
        if not valid_user:
            flash(user_error, "error")
            return redirect(url_for('signup'))

        valid_pass, pass_error = validate_password(password)
        if not valid_pass:
            flash(pass_error, "error")
            return redirect(url_for('signup'))

        if password != password_confirm:
            flash("Passwords do not match.", "error")
            return redirect(url_for('signup'))

        conn = get_db_connection()
        max_rating_id = conn.execute('SELECT MAX(userId) FROM ratings').fetchone()[0] or 610
        max_user_id = conn.execute('SELECT MAX(userId) FROM users').fetchone()[0] or 610
        new_user_id = max(max_rating_id, max_user_id) + 1

        hashed_password = generate_password_hash(password)
        timestamp = int(time.time())

        try:
            conn.execute(
                'INSERT INTO users (userId, username, password, created_at, is_anonymous) VALUES (?, ?, ?, ?, ?)',
                (new_user_id, username, hashed_password, timestamp, 0)
            )
            conn.commit()

            session['userId'] = new_user_id
            session['username'] = username
            session['is_anonymous'] = False

            flash(f"Account created! Welcome, {username}.", "success")
            conn.close()
            return redirect(url_for('browse_movies'))

        except IntegrityError:
            conn.close()
            flash("Username already exists. Please choose another.", "error")
            return redirect(url_for('signup'))

    return render_template('signup.html')


@app.route('/logout')
def logout():
    username = session.get('username', 'Guest')
    session.clear()
    flash(f"Goodbye, {username}!", "success")
    return redirect(url_for('home'))


@app.route('/profile')
@login_required
def profile():
    if session.get('is_anonymous'):
        flash("Create an account to access your profile!", "error")
        return redirect(url_for('signup'))

    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE userId = ?', (session['userId'],)).fetchone()

    rating_count = conn.execute('SELECT COUNT(*) FROM ratings WHERE userId = ?', (session['userId'],)).fetchone()[0]

    avg_rating = conn.execute('SELECT AVG(rating) FROM ratings WHERE userId = ?', (session['userId'],)).fetchone()[0]

    genre_query = """
        SELECT m.genres, AVG(r.rating) as avg_rating, COUNT(*) as count
        FROM ratings r
        JOIN movies m ON r.movieId = m.movieId
        WHERE r.userId = ?
        GROUP BY m.genres
        ORDER BY avg_rating DESC
        LIMIT 5
    """
    top_genres = conn.execute(genre_query, (session['userId'],)).fetchall()

    recent_ratings = conn.execute("""
        SELECT m.title, m.genres, r.rating, r.timestamp
        FROM ratings r
        JOIN movies m ON r.movieId = m.movieId
        WHERE r.userId = ?
        ORDER BY r.timestamp DESC
        LIMIT 10
    """, (session['userId'],)).fetchall()

    conn.close()

    return render_template('profile.html',
                           user=user,
                           rating_count=rating_count,
                           avg_rating=avg_rating,
                           top_genres=top_genres,
                           recent_ratings=recent_ratings)


@app.route('/movies')
def browse_movies():
    if 'userId' not in session:
        conn = get_db_connection()
        max_rating_id = conn.execute('SELECT MAX(userId) FROM ratings').fetchone()[0] or 610
        max_user_id = conn.execute('SELECT MAX(userId) FROM users').fetchone()[0] or 610
        new_user_id = max(max_rating_id, max_user_id) + 1

        timestamp = int(time.time())
        try:
            conn.execute(
                'INSERT INTO users (userId, username, password, created_at, is_anonymous) VALUES (?, ?, ?, ?, ?)',
                (new_user_id, f'guest_{new_user_id}', '', timestamp, 1)
            )
            conn.commit()
        except:
            pass

        conn.close()
        session['userId'] = new_user_id
        session['is_anonymous'] = True
        flash(f"Welcome! You've been assigned temporary ID: {new_user_id}", "success")

    conn = get_db_connection()
    movies_df = pd.read_sql_query("SELECT movieId, title, genres FROM movies LIMIT 100", conn)
    conn.close()
    movie_list = movies_df.to_dict('records')
    return render_template('movies.html', movies=movie_list)


@app.route('/add_rating', methods=['POST'])
def add_rating():
    if 'userId' not in session:
        return redirect(url_for('browse_movies'))
    try:
        user_id = session['userId']
        movie_id = int(request.form['movieId'])
        rating = float(request.form['rating'])
        timestamp = int(time.time())

        conn = get_db_connection()
        conn.execute(
            'INSERT OR REPLACE INTO ratings (userId, movieId, rating, timestamp) VALUES (?, ?, ?, ?)',
            (user_id, movie_id, rating, timestamp)
        )
        conn.commit()
        conn.close()
        flash(f"Your rating of {rating} ⭐ has been saved!", "success")
    except (ValueError, KeyError):
        flash("Invalid rating submission.", "error")
    return redirect(url_for('browse_movies'))


@app.route('/edit_rating', methods=['POST'])
def edit_rating():
    if 'userId' not in session:
        flash("Please log in first!", "error")
        return redirect(url_for('home'))

    try:
        user_id = session['userId']
        movie_id = int(request.form['movieId'])
        new_rating = float(request.form['rating'])
        timestamp = int(time.time())

        if new_rating < 0.5 or new_rating > 5.0:
            flash("Rating must be between 0.5 and 5.0.", "error")
            return redirect(url_for('my_ratings'))

        conn = get_db_connection()
        cursor = conn.execute(
            'UPDATE ratings SET rating = ?, timestamp = ? WHERE userId = ? AND movieId = ?',
            (new_rating, timestamp, user_id, movie_id)
        )

        if cursor.rowcount == 0:
            flash("Rating not found.", "error")
        else:
            movie = conn.execute('SELECT title FROM movies WHERE movieId = ?', (movie_id,)).fetchone()
            movie_title = movie['title'] if movie else f"Movie #{movie_id}"
            flash(f"Updated rating for '{movie_title}' to {new_rating} ⭐", "success")

        conn.commit()
        conn.close()
    except (ValueError, KeyError) as e:
        flash("Invalid rating update.", "error")

    return redirect(url_for('my_ratings'))


@app.route('/delete_rating/<int:movie_id>', methods=['POST'])
def delete_rating(movie_id):
    if 'userId' not in session:
        flash("Please log in first!", "error")
        return redirect(url_for('home'))

    try:
        user_id = session['userId']
        conn = get_db_connection()
        movie = conn.execute('SELECT title FROM movies WHERE movieId = ?', (movie_id,)).fetchone()
        movie_title = movie['title'] if movie else f"Movie #{movie_id}"

        cursor = conn.execute(
            'DELETE FROM ratings WHERE userId = ? AND movieId = ?',
            (user_id, movie_id)
        )

        if cursor.rowcount == 0:
            flash("Rating not found.", "error")
        else:
            flash(f"Deleted rating for '{movie_title}'.", "success")

        conn.commit()
        conn.close()
    except Exception as e:
        flash("Error deleting rating.", "error")

    return redirect(url_for('my_ratings'))


@app.route('/api/search')
def search_movies():
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

    print(f"Generating hybrid recommendations for user {user_id}...")
    predictions = recommender.get_recommendations(user_id, n=10)

    recommendations = []
    if predictions:
        movie_ids = [pred[0] for pred in predictions]
        conn = get_db_connection()

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
    similar = recommender.get_similar_movies(movie_id, n=10)

    if not similar:
        flash(f"Movie ID {movie_id} not found.", "error")
        return redirect(url_for('browse_movies'))

    movie_ids = [sim[0] for sim in similar]
    conn = get_db_connection()

    original_movie = pd.read_sql_query(
        f"SELECT title, genres FROM movies WHERE movieId = {movie_id}",
        conn
    ).iloc[0]

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
                'similarity': round(similarity * 100, 1)
            })

    return render_template('similar.html',
                           original_title=original_movie['title'],
                           original_genres=original_movie['genres'],
                           similar_movies=similar_list)


@app.route('/explain/<int:movie_id>')
def explain_recommendation(movie_id):
    if 'userId' not in session:
        flash("Please log in first!", "error")
        return redirect(url_for('home'))

    user_id = session['userId']
    explanation = recommender.explain_recommendation(user_id, movie_id)

    return jsonify(explanation)


@app.route('/my-ratings')
def my_ratings():
    if 'userId' not in session:
        flash("You haven't rated any movies yet.", "error")
        return redirect(url_for('browse_movies'))

    user_id = session['userId']
    conn = get_db_connection()
    query = """
        SELECT m.movieId, m.title, m.genres, r.rating
        FROM ratings r JOIN movies m ON r.movieId = m.movieId
        WHERE r.userId = ? ORDER BY r.timestamp DESC
    """
    my_ratings_df = pd.read_sql_query(query, conn, params=(user_id,))
    conn.close()
    my_ratings_list = my_ratings_df.to_dict('records')

    return render_template('my_ratings.html', ratings=my_ratings_list)


if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)