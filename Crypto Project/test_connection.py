import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime

def test_database_connection():
    """Comprehensive test of PostgreSQL database connection and basic operations"""
    
    # Load environment variables
    load_dotenv('env')  # Use 'env' since that's your filename
    
    print("=" * 60)
    print("POSTGRESQL DATABASE CONNECTION TEST")
    print("=" * 60)
    print(f"Timestamp: {datetime.now()}")
    print()
    
    # Display connection parameters (without password)
    print("Connection Parameters:")
    print(f"  Host: {os.getenv('POSTGRES_HOST')}")
    print(f"  Port: {os.getenv('POSTGRES_PORT')}")
    print(f"  Database: {os.getenv('POSTGRES_DB')}")
    print(f"  User: {os.getenv('POSTGRES_USER')}")
    password = os.getenv('POSTGRES_PASSWORD')
    print(f"  Password: {'*' * len(password) if password else 'None'}")
    print()
    
    connection = None
    cursor = None
    
    try:
        # Test 1: Basic Connection
        print("🔗 TEST 1: Establishing connection...")
        connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        print("✅ Connection successful!")
        
        # Test 2: Create cursor
        print("\n🎯 TEST 2: Creating cursor...")
        cursor = connection.cursor()
        print("✅ Cursor created!")
        
        # Test 3: Check PostgreSQL version
        print("\n📊 TEST 3: Checking PostgreSQL version...")
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✅ PostgreSQL Version: {version}")
        
        # Test 4: List existing databases
        print("\n📋 TEST 4: Listing available databases...")
        cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        databases = cursor.fetchall()
        print("✅ Available databases:")
        for db in databases:
            print(f"   - {db[0]}")
        
        # Test 5: List existing tables in current database
        print("\n📂 TEST 5: Listing tables in current database...")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        if tables:
            print("✅ Existing tables:")
            for table in tables:
                print(f"   - {table[0]}")
        else:
            print("ℹ️  No tables found in the database")
        
        # Test 6: Create a test table
        print("\n🔨 TEST 6: Creating a test table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS connection_test (
                id SERIAL PRIMARY KEY,
                test_message TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        connection.commit()
        print("✅ Test table created successfully!")
        
        # Test 7: Insert test data
        print("\n📝 TEST 7: Inserting test data...")
        cursor.execute("""
            INSERT INTO connection_test (test_message) 
            VALUES (%s);
        """, (f"Connection test at {datetime.now()}",))
        connection.commit()
        print("✅ Test data inserted!")
        
        # Test 8: Query test data
        print("\n📖 TEST 8: Querying test data...")
        cursor.execute("SELECT * FROM connection_test ORDER BY created_at DESC LIMIT 5;")
        rows = cursor.fetchall()
        print("✅ Recent test records:")
        for row in rows:
            print(f"   ID: {row[0]}, Message: {row[1]}, Created: {row[2]}")
        
        # Test 9: Count total records
        print("\n🔢 TEST 9: Counting total test records...")
        cursor.execute("SELECT COUNT(*) FROM connection_test;")
        count = cursor.fetchone()[0]
        print(f"✅ Total records in test table: {count}")
        
        # Test 10: Check user permissions
        print("\n🔐 TEST 10: Checking user permissions...")
        cursor.execute("""
            SELECT 
                table_name,
                privilege_type 
            FROM information_schema.role_table_grants 
            WHERE grantee = %s 
            LIMIT 5;
        """, (os.getenv('POSTGRES_USER'),))
        permissions = cursor.fetchall()
        if permissions:
            print("✅ User permissions (sample):")
            for perm in permissions:
                print(f"   Table: {perm[0]}, Permission: {perm[1]}")
        else:
            print("ℹ️  No specific table permissions found (user might have broader permissions)")
        
        print("\n" + "=" * 60)
        print("🎉 ALL TESTS PASSED! Database connection is working perfectly!")
        print("=" * 60)
        
    except psycopg2.Error as e:
        print(f"\n❌ PostgreSQL Error: {e}")
        print(f"Error Code: {e.pgcode if hasattr(e, 'pgcode') else 'N/A'}")
        
    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
        
    finally:
        # Clean up connections
        if cursor:
            cursor.close()
            print("\n🧹 Cursor closed")
        if connection:
            connection.close()
            print("🧹 Connection closed")
        print("\n✅ Cleanup completed")

if __name__ == "__main__":
    test_database_connection()