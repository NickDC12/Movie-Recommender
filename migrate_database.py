import sqlite3
import os

DATABASE_NAME = 'movielens.db'


def migrate_database():
    """Add users table to existing database"""

    # Check if database exists
    if not os.path.exists(DATABASE_NAME):
        print(f"❌ Error: {DATABASE_NAME} not found!")
        print("Make sure you're running this from the same directory as your database.")
        return False

    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Check if users table already exists
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='users'
    """)

    if cursor.fetchone():
        print("✅ Users table already exists! No migration needed.")
        conn.close()
        return True

    # Create users table
    print("Creating users table...")
    cursor.execute('''
        CREATE TABLE users (
            userId INTEGER PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            is_anonymous BOOLEAN DEFAULT 0
        )
    ''')

    conn.commit()

    # Verify it was created
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='users'
    """)

    if cursor.fetchone():
        print("✅ Users table created successfully!")

        # Show current tables
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table'
            ORDER BY name
        """)
        tables = cursor.fetchall()
        print(f"\n📊 Current tables in database:")
        for table in tables:
            print(f"   - {table[0]}")

        conn.close()
        return True
    else:
        print("❌ Error: Failed to create users table")
        conn.close()
        return False


if __name__ == "__main__":
    print("=" * 50)
    print("DATABASE MIGRATION SCRIPT")
    print("=" * 50)
    print()

    success = migrate_database()

    print()
    if success:
        print("✅ Migration completed successfully!")
        print("You can now use the signup/login features.")
    else:
        print("❌ Migration failed. Please check the error messages above.")
    print()