import pandas as pd
import snowflake.connector
import requests
import config


def fetch_data_from_alpha_vantage(symbol):
    """
    Fetch daily stock data from Alpha Vantage for the given symbol.
    Returns a DataFrame containing the latest stock data.
    """
    url = (
        f"https://www.alphavantage.co/query?"
        f"function=TIME_SERIES_DAILY&"
        f"symbol={symbol}&"
        f"apikey={config.ALPHAVANTAGE_API_KEY}&"
        f"outputsize=compact"
    )

    response = requests.get(url)
    data = response.json()

    time_series = data.get("Time Series (Daily)", {})
    if not time_series:
        print(f"No 'Time Series (Daily)' data found for {symbol}.")
        return pd.DataFrame()

    records = []
    for date_str, daily_data in time_series.items():
        records.append({
            "TRADE_DATE": date_str,
            "OPEN": float(daily_data["1. open"]),
            "HIGH": float(daily_data["2. high"]),
            "LOW": float(daily_data["3. low"]),
            "CLOSE": float(daily_data["4. close"]),
            "VOLUME": float(daily_data["5. volume"]),
        })

    df = pd.DataFrame(records)
    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"])
    df.sort_values(by="TRADE_DATE", inplace=True)
    df = df.tail(251)
    
    df["MA10"] = df["CLOSE"].rolling(window=10).mean()
    df["MA50"] = df["CLOSE"].rolling(window=50).mean()

    df["SIGNAL"] = "HOLD"
    df.loc[(df["MA10"] > df["MA50"]), "SIGNAL"] = "BUY"
    df.loc[(df["MA10"] < df["MA50"]), "SIGNAL"] = "SELL"

    return df


def fetch_existing_dates(symbol):
    """
    Fetch existing TRADE_DATEs for a given symbol from the Snowflake table.
    """
    conn = snowflake.connector.connect(
        user=config.SNOWFLAKE_USER,
        password=config.SNOWFLAKE_PASSWORD,
        account=config.SNOWFLAKE_ACCOUNT,
        database=config.SNOWFLAKE_DATABASE,
        schema=config.SNOWFLAKE_SCHEMA,
        warehouse=config.SNOWFLAKE_WAREHOUSE
    )
    cursor = conn.cursor()

    query = f"""
        SELECT TRADE_DATE
        FROM STOCK_DATA
        WHERE SYMBOL = '{symbol}'
    """
    cursor.execute(query)
    existing_dates = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()
    return existing_dates


def insert_new_data_into_snowflake(df, symbol, table_name="STOCK_DATA"):
    """
    Insert only new stock data into Snowflake, avoiding redundancy.
    """
    if df.empty:
        print(f"No data to insert for {symbol}.")
        return

    existing_dates = fetch_existing_dates(symbol)

    df_new = df[~df["TRADE_DATE"].isin(existing_dates)]

    if df_new.empty:
        print(f"All data for {symbol} is already up-to-date. No new rows to insert.")
        return

    conn = snowflake.connector.connect(
        user=config.SNOWFLAKE_USER,
        password=config.SNOWFLAKE_PASSWORD,
        account=config.SNOWFLAKE_ACCOUNT,
        database=config.SNOWFLAKE_DATABASE,
        schema=config.SNOWFLAKE_SCHEMA,
        warehouse=config.SNOWFLAKE_WAREHOUSE
    )
    cursor = conn.cursor()

    sql = f"""
        INSERT INTO {table_name} (
            SYMBOL, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, MA10, MA50, SIGNAL
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    data_to_insert = []
    for row in df_new.itertuples(index=False):
        data_to_insert.append((
            symbol,                               
            row.TRADE_DATE.strftime('%Y-%m-%d'),  
            float(row.OPEN),
            float(row.HIGH),
            float(row.LOW),
            float(row.CLOSE),
            float(row.VOLUME),
            float(row.MA10) if not pd.isna(row.MA10) else None,
            float(row.MA50) if not pd.isna(row.MA50) else None,
            str(row.SIGNAL)
        ))

    cursor.executemany(sql, data_to_insert)
    conn.commit()

    print(f"Inserted {len(data_to_insert)} new rows into {table_name} for symbol {symbol}.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Error: Please provide a stock symbol as an argument.")
        sys.exit(1)

    symbol = sys.argv[1].upper()

    df = fetch_data_from_alpha_vantage(symbol)
    insert_new_data_into_snowflake(df, symbol)
