import sqlite3
import pandas as pd

def load_sql_table (table, path) :
    conn = sqlite3.connect(path)

    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    
    try:
        df['Date'] = pd.to_datetime(df['Date'])
    except:
        pass

    conn.close()
    
    return df

# test = load_sql_table('TICKERS_ALL',sql_file_path)

def update_db(table, df_output, path):
    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    df_output.to_sql(table, conn, if_exists='replace', index=False)

    # Step 4: (Optional) Verify that the table has been created and populated
    cursor.execute(f"SELECT * FROM {table} LIMIT 5")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

    # Step 5: Commit and close the connection
    conn.commit()
    conn.close()

# update_db('TICKERS_ALL',all_tickers,sql_file_path)

def list_tables(path):
    # Connect to the SQLite database
    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    # Execute a query to get the names of all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    
    # Fetch all results
    tables = cursor.fetchall()
    
    # Print the names of all tables
    print(f"Tables in the {path}:")
    for table in tables:
        print(table[0])

    conn.close()

# db_path = sql_file_path
# list_tables(db_path)
