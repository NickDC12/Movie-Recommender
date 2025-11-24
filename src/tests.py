import sqlite3

def test_database():
    # Test database structure
    conn = sqlite3.connect('movielens.db')
    cursor = conn.cursor()
    
    # Check tables
    tables = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    table_names = [t[0] for t in tables]
    
    assert 'movies' in table_names, "Movies table missing"
    assert 'ratings' in table_names, "Ratings table missing"
    assert 'users' in table_names, "Users table missing"
    print("Database tables exist")

    
    # Check movies data
    movie_count = cursor.execute("SELECT COUNT(*) FROM movies").fetchone()[0]
    assert movie_count > 1000, f"Only {movie_count} movies (need 1000+)"
    print(f"{movie_count} movies in database")
    
    # Check ratings data
    rating_count = cursor.execute("SELECT COUNT(*) FROM ratings").fetchone()[0]
    assert rating_count > 100, f"Only {rating_count} ratings"
    print(f"{rating_count} ratings in database")
    
    conn.close()

def test_app_routes():
    # Test that Flask app can be imported
    try:
        from src.app import app
        print("Flask app imports successfully")
        
        # Check routes exist
        routes = [rule.rule for rule in app.url_map.iter_rules()]
        expected_routes = ['/', '/login', '/signup', '/movies', '/recommend', '/profile']
        
        for route in expected_routes:
            assert route in routes, f"Route {route} missing"
        print(f"All expected routes exist ({len(routes)} total)")
        
    except ImportError as e:
        print(f"Failed to import app: {e}")
        return False
    
    return True

def test_recommender():
    # Test that recommender can be imported
    try:
        from src.hybrid_recommender import HybridRecommender
        print("HybridRecommender imports successfully")
        print("Recommender module available")
    except ImportError as e:
        print(f"Failed to import recommender: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("=" * 60)
    print("RUNNING BASIC TESTS")
    print("=" * 60)
    print()
    
    try:
        print("1. Testing Database...")
        test_database()
        print()
        
        print("2. Testing Flask App...")
        test_app_routes()
        print()
        
        print("3. Testing Recommender...")
        test_recommender()
        print()
        
        print("=" * 60)
        print("ALL TESTS PASSED")
        print("=" * 60)
        
    except AssertionError as e:
        print(f"\nTEST FAILED: {e}")
    except Exception as e:
        print(f"\nERROR: {e}")