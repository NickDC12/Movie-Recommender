import sqlite3

# connect to database
conn = sqlite3.connect('movielens.db')

# check for duplicate ratings
duplicates = conn.execute("""
    SELECT userId, movieId, COUNT(*) as count
    FROM ratings
    GROUP BY userId, movieId
    HAVING count > 1
""").fetchall()

if duplicates:
    print(f"⚠️  WARNING: Found {len(duplicates)} duplicate ratings!")
    print("\nDuplicates found:")
    for user_id, movie_id, count in duplicates[:5]:
        print(f"   User {user_id}, Movie {movie_id}: {count} entries")
    if len(duplicates) > 5:
        print(f"   ... and {len(duplicates) - 5} more")
    print("\n💡 Run 'python fix_duplicates.py' to clean them up")
else:
    print("✅ No duplicate ratings found - database is clean!")

conn.close()