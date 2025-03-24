import sqlite3
import pandas as pd
import chardet
import requests
import os

def download_and_clean_fx_data():
    # Define CSV URL for FX data
    csv_url = "https://data.snb.ch/api/cube/devkum/data/csv/de"

    # Download CSV file
    response = requests.get(csv_url)
    raw_data = response.content

    # Detect encoding
    result = chardet.detect(raw_data)
    detected_encoding = result["encoding"]
    print(f"Detected encoding: {detected_encoding}")

    # Read CSV using detected encoding
    df = pd.read_csv(csv_url, sep=";", encoding=detected_encoding, skiprows=3)

    # Check the row count of the dataframe
    row_count = len(df)
    print(f"Number of rows in the DataFrame: {row_count}")

    # Only keep the end-of-month data points
    df = df[df["D0"] == "M1"]

    # Check the row count of the dataframe after filtering
    row_count = len(df)
    print(f"Number of rows in the DataFrame after filtering for end-of-month data: {row_count}")

    # Remove rows where 'Value' is NA
    df = df.dropna(subset=["Value"])

    # Check the row count of the dataframe after dropping NA values
    row_count = len(df)
    print(f"Number of rows in the DataFrame after dropping NA values: {row_count}")

    # Convert 'Date' column to the first day of the month, then shift to last day of the month
    df["Date"] = (pd.to_datetime(df["Date"], format="%Y-%m", errors="coerce") + pd.offsets.MonthEnd(0)).dt.date
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")  # Convert to datetime

    # Check the row count of the dataframe after date conversion
    row_count = len(df)
    print(f"Number of rows in the DataFrame after date conversion: {row_count}")

    # Remove rows where 'Date' is NaT (null after conversion)
    df = df.dropna(subset=["Date"])

    # Check the row count of the dataframe after dropping NaT in 'Date' column
    row_count = len(df)
    print(f"Number of rows in the DataFrame after dropping NaT in 'Date' column: {row_count}")

    # Check for any remaining NULL values in other columns
    if df.isnull().values.any():
        raise ValueError("Data contains NULL values in columns other than 'Date'. Fix the source data.")

    # Drop the end-of-month (M1) vs. mid-of-month (M0) exchange rate identifier
    df = df.drop(columns=["D0"])

    # Rename currency column
    df = df.rename(columns={"D1": "Currency"})

    # Convert Date column to string (YYYY-MM-DD format)
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

    return df


def download_and_clean_market_data():
    # Define CSV URL for market data
    csv_url = "https://www.six-group.com/fqs/closing.csv?select=ShortName,ISIN,ValorSymbol,ValorNumber,ClosingPrice,LatestTradeDate,TotalVolume,TradingBaseCurrency,Exchange&where=ProductLine=ET&pagesize=10000"

    # Download CSV file
    response = requests.get(csv_url)
    raw_data = response.content

    # Detect encoding
    result = chardet.detect(raw_data)
    detected_encoding = result["encoding"]
    print(f"Detected encoding: {detected_encoding}")

    # Read CSV using detected encoding
    df = pd.read_csv(csv_url, sep=";", encoding=detected_encoding)

    # Check the row count of the dataframe
    row_count = len(df)
    print(f"Number of rows in the DataFrame: {row_count}")

    if row_count >= 10000:
        raise ValueError("Data contains more than 10,000 rows. Adjust the page size in the URL.")

    # Convert 'LatestTradeDate' to proper date format, handling errors
    df["LatestTradeDate"] = pd.to_datetime(df["LatestTradeDate"], format="%Y%m%d", errors="coerce").dt.date
    df["LatestTradeDate"] = pd.to_datetime(df["LatestTradeDate"], errors="coerce")  # Convert to datetime

    # Remove rows where 'LatestTradeDate' is NaT (null after conversion)
    df = df.dropna(subset=["LatestTradeDate"])

    # Check for any remaining NULL values in other columns
    if df.isnull().values.any():
        raise ValueError("Data contains NULL values in columns other than 'LatestTradeDate'. Fix the source data.")

    # Convert 'LatestTradeDate' column to string (YYYY-MM-DD format)
    df["LatestTradeDate"] = df["LatestTradeDate"].dt.strftime("%Y-%m-%d")

    return df


def insert_fx_data_to_db(df):
    # Define the path to the database file
    db_path = os.path.join(os.path.dirname(__file__), '..', '..', 'db', 'market_data.db')

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table with COMPOUND PRIMARY KEY
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fx_data (
            Date TEXT NOT NULL,
            Currency TEXT NOT NULL,
            Value REAL NOT NULL,
            PRIMARY KEY (Date, Currency)
        )
    """)

    # Prepare UPSERT SQL statement
    upsert_query = """
        INSERT INTO fx_data (Date, Currency, Value)
        VALUES (?, ?, ?)
        ON CONFLICT(Date, Currency)
        DO UPDATE SET 
            Value = excluded.Value;
    """

    # Convert DataFrame rows to a list of tuples
    data_tuples = df.to_records(index=False).tolist()

    # Execute UPSERT in batches
    cursor.executemany(upsert_query, data_tuples)

    # Commit and close connection
    conn.commit()
    conn.close()

    print("FX data downloaded, cleaned, and inserted into SQLite with UPSERT (update on conflict).")


def insert_market_data_to_db(df):
    # Define the path to the database file
    db_path = os.path.join(os.path.dirname(__file__), '..', '..', 'db', 'market_data.db')

    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table with COMPOUND PRIMARY KEY
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            ShortName TEXT NOT NULL,
            ISIN TEXT NOT NULL,
            ValorSymbol TEXT NOT NULL,
            ValorNumber INTEGER NOT NULL,
            ClosingPrice REAL NOT NULL,
            LatestTradeDate TEXT NOT NULL,
            TotalVolume INTEGER NOT NULL,
            TradingBaseCurrency TEXT NOT NULL,
            Exchange TEXT NOT NULL,
            PRIMARY KEY (ShortName, ISIN, ValorSymbol, ValorNumber, LatestTradeDate, TradingBaseCurrency, Exchange)
        )
    """)

    # Prepare UPSERT SQL statement
    upsert_query = """
        INSERT INTO market_data (ShortName, ISIN, ValorSymbol, ValorNumber, ClosingPrice, LatestTradeDate, TotalVolume, TradingBaseCurrency, Exchange)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ShortName, ISIN, ValorSymbol, ValorNumber, LatestTradeDate, TradingBaseCurrency, Exchange)
        DO UPDATE SET
            ClosingPrice = excluded.ClosingPrice,
            TotalVolume = excluded.TotalVolume;
    """

    # Convert DataFrame rows to a list of tuples
    data_tuples = df.to_records(index=False).tolist()

    # Execute UPSERT in batches
    cursor.executemany(upsert_query, data_tuples)

    # Commit and close connection
    conn.commit()
    conn.close()

    print("Market data downloaded, cleaned, and inserted into SQLite with UPSERT (update on conflict).")


if __name__ == "__main__":
    # Step 1: Download and clean FX data
    fx_df = download_and_clean_fx_data()

    # Step 2: Insert FX data into SQLite database
    insert_fx_data_to_db(fx_df)

    # Step 3: Download and clean Market data
    market_df = download_and_clean_market_data()

    # Step 4: Insert Market data into SQLite database
    insert_market_data_to_db(market_df)
