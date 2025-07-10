#!/usr/bin/env python3

import sqlite3
import pandas as pd
import os

def export_db_to_excel():
    """Export SQLite database to Excel format"""
    
    # Connect to the database
    db_path = '/mnt/c/Users/bbofo/OneDrive/Desktop/ChatMRPT/instance/interactions.db'
    
    try:
        conn = sqlite3.connect(db_path)
        
        # Get all table names
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        # Create Excel writer
        with pd.ExcelWriter('interactions_database.xlsx', engine='openpyxl') as writer:
            for table in tables:
                # Read each table into a DataFrame
                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                
                # Write to Excel sheet
                df.to_excel(writer, sheet_name=table, index=False)
                print(f"Exported table '{table}' with {len(df)} rows")
        
        conn.close()
        print(f"Database exported to 'interactions_database.xlsx'")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    export_db_to_excel()