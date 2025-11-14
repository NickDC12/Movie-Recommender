import sqlite3

# connect to database
conn = sqlite3.connect('movielens.db')
cursor = conn.cursor()

print("Checking for duplicate ratings...")

# find duplicates
duplicates = cursor.execute("""
                            SELECT userId, movieId, COUNT(*) as count
                            FROM ratings
                            GROUP BY userId, movieId
                            HAVING count > 1
                            """).fetchall()

if not duplicates:
    print("✓ No duplicates found!")
    conn.close()
    exit()

print(f"Found {len(duplicates)} duplicate entries")

# keep only the most recent entry
for user_id, movie_id, count in duplicates:
    print(f"Fixing User {user_id}, Movie {movie_id} ({count} entries)...")

    # get all ratings for this user-movie pair
    ratings = cursor.execute("""
                             SELECT rowid, rating, timestamp
                             FROM ratings
                             WHERE userId = ? AND movieId = ?
                             ORDER BY timestamp DESC
                             """, (user_id, movie_id)).fetchall()

    # keep the most recent, delete the rest
    keep_rowid = ratings[0][0]
    keep_rating = ratings[0][1]

    # delete all entries
    cursor.execute("DELETE FROM ratings WHERE userId = ? AND movieId = ?",
                   (user_id, movie_id))

    # re-insert the most recent one
    cursor.execute("""
                   INSERT INTO ratings (userId, movieId, rating, timestamp)
                   VALUES (?, ?, ?, ?)
                   """, (user_id, movie_id, keep_rating, ratings[0][2]))

    print(f"   Kept rating {keep_rating} from most recent entry")

conn.commit()
print(f"\n✓ Fixed {len(duplicates)} duplicates!")

# verify
remaining = cursor.execute("""
                           SELECT COUNT(*)
                           FROM (SELECT userId, movieId, COUNT(*) as count
                                 FROM ratings
                                 GROUP BY userId, movieId
                                 HAVING count > 1)
                           """).fetchone()[0]

print(f"Remaining duplicates: {remaining}")

conn.close()