# ─────────────────────────────────────────────────────────────
# fetch_smard.py
# Purpose: Fetch German electricity generation data from the
#          SMARD API and load it into PostgreSQL
# Source:  https://www.smard.de (Official
#          German government energy data)
# ─────────────────────────────────────────────────────────────

# Standard library imports
import requests                          # Makes HTTP requests to APIs
import psycopg2                          # Connects to PostgreSQL databases
from datetime import datetime, timedelta # Handles date and time calculations
from dotenv import load_dotenv           # Loads environment variables from a .env file
import os                                # Access environment variables

# Load database credentials from .env file
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")


# Database connection function
def get_connection():
    """
    Establishes a connection to the PostgreSQL database using credentials
    stored in environment variables. This keeps sensitive info out of the code.
    """
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),         
        port=os.getenv("DB_PORT"),         
        dbname=os.getenv("DB_NAME"),       
        user=os.getenv("DB_USER"),         
        password=os.getenv("DB_PASSWORD")  
    )


#Energy type filter IDs from SMARD API
# SMARD identifies each energy source by a numeric filter ID.
# This dictionary maps those IDs to human-readable names.
# These IDs are official SMARD API filter codes.
ENERGY_TYPES = {
    4066: "Wind Offshore", 
    4067: "Wind Onshore",   
    4068: "Solar",          
    4069: "Hydropower",    
    4071: "Biomass",        
    4072: "Nuclear",        
    4073: "Brown Coal",    
    4074: "Hard Coal",      
    4075: "Natural Gas",    
    4076: "Oil",            
}


# SMARD API endpoint template:
# {filter} = energy type filter ID
# {timestamp} = the start of the week in milliseconds
# DE = Germany region code
BASE_URL = (
    "https://www.smard.de/app/chart_data/"
    "{filter}/DE/{filter}_DE_quarterhour_{timestamp}.json"
)


#Timestamp conversion function
def to_smard_timestamp(dt: datetime) -> int:
    """
    Converts any datetime to SMARD's required format:
    - Must be the start of a Monday (SMARD publishes weekly chunks)
    - Must be in milliseconds (not seconds)
    """
    # Roll back to the most recent Monday
    # dt.weekday() returns 0 for Monday, 1 for Tuesday, ..., 6 for Sunday
    monday = dt - timedelta(days=dt.weekday())

    # Set time to exactly midnight (00:00:00)
    # SMARD needs start of day, not mid-day
    monday = monday.replace(hour=0, minute=0, second=0, microsecond=0)

    # Convert to Unix timestamp (seconds) then multiply by 1000
    # for milliseconds — SMARD's expected format
    return int(monday.timestamp()) * 1000


#Function to fetch energy data from SMARD API
def fetch_energy_data(filter_id: int, timestamp_ms: int) -> list:
    """
    Fetches the energy generation data series for a specific energy type
    and week from the SMARD API. Returns a list of [timestamp_ms, value] pairs.
    """
    # Build the full API URL for this energy type and week
    url = BASE_URL.format(filter=filter_id, timestamp=timestamp_ms)

    # Make the HTTP GET request with a 10 second timeout
    response = requests.get(url, timeout=10)

    # Check if the request was successful (HTTP 200 OK)
    if response.status_code != 200:
        print(f"Failed to fetch filter {filter_id}: HTTP {response.status_code}")
        return []  # Return empty list so pipeline continues

    # Parse JSON response into Python dictionary
    data = response.json()

    # The 'series' key contains the time series data as a list of [timestamp_ms, value] pairs
    return data.get("series", [])


#Main ingestion function
def ingest():
    """
    Main function to orchestrate the entire ingestion process:
1. Connect to the database
2. Loop through each energy type and fetch data from SMARD
3. Clean and prepare the data for insertion
4. Insert the data into the raw_energy table
5. Commit the transaction and close the connection
    """
    print(f"Starting ingestion at {datetime.now()}")

    # Establish database connection and create a cursor for executing SQL commands
    conn = get_connection()
    cur = conn.cursor()

    # Calculate the timestamp for 7 days ago (start of the week) in SMARD's required format
    target_date = datetime.now() - timedelta(days=7)
    timestamp_ms = to_smard_timestamp(target_date)

    # Counter to track total rows inserted across all energy types
    total_inserted = 0

    # Loop through each energy type defined in ENERGY_TYPES
    for filter_id, energy_name in ENERGY_TYPES.items():
        print(f"Fetching: {energy_name}")

        # Fetch the energy data series for this filter ID and timestamp
        series = fetch_energy_data(filter_id, timestamp_ms)

        # Clean and prepare the data for insertion into the database
        rows = []
        for entry in series:
            ts_ms, value = entry  # Unpack timestamp and energy value from the API response

            # Skip missing values (SMARD sometimes has data gaps)
            if value is None:
                continue

            # Convert milliseconds → normal datetime
            ts = datetime.fromtimestamp(ts_ms / 1000)

            # Convert MWh per 15min → MW (standard power unit)
            rows.append((ts, energy_name, round(value / 4, 2)))

        # Insert the cleaned data into the raw_energy table using executemany for efficiency
        if rows:  # Only insert if we have data
            cur.executemany("""
                INSERT INTO raw_energy (timestamp, energy_type, production_mwh)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
            """, rows)
            # ON CONFLICT DO NOTHING = skip duplicates silently

            total_inserted += len(rows)
            print(f"{len(rows)} rows inserted for {energy_name}")

    # Commit the transaction to save all changes to the database
    conn.commit()   
    cur.close()     
    conn.close()    

    print(f"\nDone, Total rows inserted: {total_inserted}")


# Run the ingestion pipeline when this script is executed directly
if __name__ == "__main__":
    ingest()