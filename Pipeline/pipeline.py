# ─────────────────────────────────────────────────────────────
# pipeline.py
# Purpose: Orchestrate the entire data pipeline:
# fetch data, transform it, and load it into the database.
# ─────────────────────────────────────────────────────────────
import psycopg2
import os
import sys
from datetime import datetime

# Add parent directory to sys.path to import fetch_smardapi.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Ingestion.fetch_smardapi import ingest

#Database connection function
def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="Energiewende",
        user="postgres",
        password="postgres123"
    )

# Function to run SQL files
def run_sql_file(filepath: str):
    print(f"Executing {filepath}...")
    with open(filepath, "r") as f:
        sql = f.read()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Done!")

# Main pipeline function
def run_pipeline():
    start = datetime.now()
    print(f"Pipeline started at {start}")

    #Ingest data from SMARD API
    print("Ingesting data from SMARD API...")
    ingest()
    print()

    #Create staging table
    print("Creating staging table...")
    run_sql_file("sql/staging.sql")
    print()

    #Create mart daily table
    print("Creating mart daily table...")
    run_sql_file("sql/mart.sql")
    print()

    #Create mart hourly table
    print("Creating mart hourly table...")
    run_sql_file("sql/mart_hour.sql")
    print()

    end = datetime.now()
    duration = (end - start).seconds
    print(f"Pipeline completed in {duration} seconds")

if __name__ == "__main__":
    run_pipeline()