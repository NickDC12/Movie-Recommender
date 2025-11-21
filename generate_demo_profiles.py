"""
Generate Demo Profiles
Creates 6-8 demo users with 10-15 ratings each for fast recommendations
Each profile has a different "personality" (action lover, romance fan, etc.)
"""

import sqlite3
import time
import random

DATABASE_NAME = 'movielens.db'

# Define demo profiles with different tastes
DEMO_PROFILES = [
    {
        'name': 'Action Fan',
        'genres': ['Action', 'Adventure', 'Thriller'],
        'rating_range': (4.0, 5.0),
        'count': 12
    },
    {
        'name': 'Comedy Lover',
        'genres': ['Comedy', 'Romance'],
        'rating_range': (3.5, 5.0),
        'count': 15
    },
    {
        'name': 'Sci-Fi Enthusiast',
        'genres': ['Sci-Fi', 'Fantasy', 'Adventure'],
        'rating_range': (4.0, 5.0),
        'count': 12
    },
    {
        'name': 'Drama Buff',
        'genres': ['Drama', 'Crime', 'Mystery'],
        'rating_range': (3.5, 5.0),
        'count': 14
    },
    {
        'name': 'Horror Fanatic',
        'genres': ['Horror', 'Thriller', 'Mystery'],
        'rating_range': (3.5, 5.0),
        'count': 10
    },
    {
        'name': 'Family Movies',
        'genres': ['Animation', 'Children', 'Comedy'],
        'rating_range': (4.0, 5.0),
        'count': 13
    },
    {
        'name': 'Classic Film Buff',
        'genres': ['Drama', 'Film-Noir', 'War'],
        'rating_range': (4.0, 5.0),
        'count': 11
    },
    {
        'name': 'Casual Viewer',
        'genres': ['Comedy', 'Action', 'Drama', 'Romance'],
        'rating_range': (3.0, 4.5),
        'count': 15
    }
]


def get_next_demo_user_id(conn):
    """Find the next available userId for demo accounts"""
    cursor = conn.cursor()

    # Find highest userId in ratings table
    max_rating_id = cursor.execute('SELECT MAX(userId) FROM ratings').fetchone()[0] or 0

    # Find highest userId in users table
    max_user_id = cursor.execute('SELECT MAX(userId) FROM users').fetchone()[0] or 0

    # Start demo users at a high number to avoid conflicts
    return max(max_rating_id, max_user_id, 10000) + 1


def get_movies_by_genres(conn, genres, limit=50):
    """Get movies matching the specified genres"""
    genre_conditions = " OR ".join([f"genres LIKE '%{genre}%'" for genre in genres])

    query = f'''
        SELECT movieId, title, genres 
        FROM movies 
        WHERE {genre_conditions}
        ORDER BY RANDOM()
        LIMIT ?
    '''

    cursor = conn.cursor()
    results = cursor.execute(query, (limit,)).fetchall()

    # Convert tuples to dictionaries
    movies = []
    for row in results:
        movies.append({
            'movieId': row[0],
            'title': row[1],
            'genres': row[2]
        })

    return movies


def create_demo_profile(conn, user_id, profile_config):
    """Create a demo user with ratings matching their profile"""
    cursor = conn.cursor()
    timestamp = int(time.time())

    # Add user to users table
    cursor.execute('''
        INSERT INTO users (userId, username, password, created_at, is_anonymous)
        VALUES (?, ?, ?, ?, ?)
    ''', (
        user_id,
        profile_config['name'],
        'demo_account',  # Placeholder password
        timestamp,
        1  # Mark as demo/anonymous
    ))

    # Get movies matching their taste
    movies = get_movies_by_genres(conn, profile_config['genres'], limit=100)

    if len(movies) < profile_config['count']:
        print(f"  ⚠️ Warning: Only found {len(movies)} movies for {profile_config['name']}")
        return 0

    # Randomly select movies and assign ratings
    selected_movies = random.sample(movies, profile_config['count'])
    rating_count = 0

    for movie in selected_movies:
        rating = round(random.uniform(*profile_config['rating_range']), 1)

        try:
            cursor.execute('''
                INSERT INTO ratings (userId, movieId, rating, timestamp)
                VALUES (?, ?, ?, ?)
            ''', (user_id, movie['movieId'], rating, timestamp))
            rating_count += 1
        except sqlite3.IntegrityError:
            # Skip if somehow this rating already exists
            pass

    return rating_count


def generate_demo_profiles():
    """Generate all demo profiles"""
    conn = sqlite3.connect(DATABASE_NAME)

    print("🎬 Generating demo profiles...\n")

    # Get starting user ID
    next_user_id = get_next_demo_user_id(conn)
    print(f"Starting demo users at ID: {next_user_id}\n")

    created_profiles = []

    for i, profile_config in enumerate(DEMO_PROFILES):
        user_id = next_user_id + i
        print(f"Creating: {profile_config['name']} (User {user_id})...")

        rating_count = create_demo_profile(conn, user_id, profile_config)

        if rating_count > 0:
            created_profiles.append({
                'user_id': user_id,
                'name': profile_config['name'],
                'ratings': rating_count
            })
            print(f"  ✅ Added {rating_count} ratings\n")
        else:
            print(f"  ❌ Failed to create profile\n")

    conn.commit()

    # Print summary
    print("=" * 60)
    print("✅ Demo Profile Generation Complete!")
    print("=" * 60)
    print(f"\nCreated {len(created_profiles)} profiles:\n")

    for profile in created_profiles:
        print(f"  • {profile['name']:<20} (User {profile['user_id']}) - {profile['ratings']} ratings")

    print("\n💡 These profiles are optimized for fast recommendations!")
    print("💡 Each has 10-15 ratings for quick demo performance")

    # Verify in database
    cursor = conn.cursor()
    demo_users = cursor.execute('''
        SELECT u.userId, u.username, COUNT(r.rating) as rating_count
        FROM users u
        LEFT JOIN ratings r ON u.userId = r.userId
        WHERE u.is_anonymous = 1
        GROUP BY u.userId
        ORDER BY u.userId DESC
        LIMIT ?
    ''', (len(DEMO_PROFILES),)).fetchall()

    print("\n📊 Verification from database:")
    for user in demo_users:
        print(f"  • User {user[0]}: {user[1]} - {user[2]} ratings")

    conn.close()

    return created_profiles


if __name__ == "__main__":
    print("=" * 60)
    print("DEMO PROFILE GENERATOR")
    print("=" * 60)
    print()

    try:
        profiles = generate_demo_profiles()
        print("\n🎉 All done! Your demo profiles are ready to use.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()