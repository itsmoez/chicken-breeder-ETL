import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd

# Load environment from .env file
load_dotenv()

# Import your cleaned DataFrame from app.py
from app import df

try:
    # STEP 1: Connect to PostgreSQL database
    connection = psycopg2.connect(
        host=os.getenv('postgres_host', 'localhost'),
        user=os.getenv('postgres_user', 'postgres'),
        password=os.getenv('postgres_pass'),
        database=os.getenv('postgres_db'),
        port=os.getenv('postgres_port', '5432')
    )
    
    print("✅ Connected to PostgreSQL database successfully!")
    
    # STEP 2: Create a cursor to execute SQL commands
    cursor = connection.cursor()
    
    # STEP 3: Read and execute the SQL file to create tables
    print("📝 Creating tables from db-load.sql...")
    print(f"🔍 Current working directory: {os.getcwd()}")
    print(f"🔍 Files in current directory: {os.listdir('.')}")

    with open('Db/db-load.sql', 'r') as sql_file:
        sql_content = sql_file.read()
    
    # Execute each SQL command in the file
    sql_commands = sql_content.split(';')
    for command in sql_commands:
        if command.strip():  # Only execute non-empty commands
            cursor.execute(command.strip())
    
    print("✅ Tables created successfully!")
    
    # STEP 4: Insert your cleaned data row by row
    print(f"📊 Loading {len(df)} chicken records...")
    
    # Loop through each row in your DataFrame
    for index, row in df.iterrows():
        # Create INSERT statement for each chicken
        insert_query = """
        INSERT INTO chickens (name, age, breed, birthday, size) 
        VALUES (%s, %s, %s, %s, %s)
        """
        
        # Execute the INSERT with the row data
        cursor.execute(insert_query, (
            row['name'],      # Chicken name
            int(row['age']),  # Chicken age (convert to integer)
            row['breed'],     # Chicken breed
            row['birthday'],  # Birthday string
            row['size']       # Size (Very Small, Small, etc.)
        ))
        
        print(f"✅ Inserted: {row['name']} - {row['breed']} - Age {row['age']}")
    
    # STEP 5: Commit all changes to the database
    connection.commit()
    print("✅ All data committed to database!")
    
    # STEP 6: Verify the data was loaded correctly
    cursor.execute("SELECT COUNT(*) FROM chickens")
    total_count = cursor.fetchone()[0]
    print(f"📊 Total chickens in database: {total_count}")
    
    # Show first 5 records to verify
    cursor.execute("SELECT id, name, age, breed FROM chickens LIMIT 5")
    sample_records = cursor.fetchall()
    
    print("\n📋 Sample records (showing ID auto-increment working):")
    for record in sample_records:
        print(f"ID: {record[0]} | Name: {record[1]} | Age: {record[2]} | Breed: {record[3]}")

except psycopg2.Error as db_error:
    print(f"❌ Database error occurred: {db_error}")
    if 'connection' in locals():
        connection.rollback()

except FileNotFoundError:
    print("❌ Could not find db-load.sql file!")

except Exception as general_error:
    print(f"❌ An error occurred: {general_error}")
    if 'connection' in locals():
        connection.rollback()

finally:
    # STEP 7: Clean up - close cursor and connection
    if 'cursor' in locals():
        cursor.close()
    if 'connection' in locals():
        connection.close()
    print("🔌 Database connection closed.")


