import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.graph_objects as go
import config
from fetch_and_load import fetch_data_from_alpha_vantage, insert_new_data_into_snowflake
from predict import train_predict_stock_model


def fetch_data_from_snowflake(symbol):
    """
    Fetch stock data from Snowflake for the given symbol.
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
        SELECT TRADE_DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, MA10, MA50, SIGNAL
        FROM STOCK_DATA
        WHERE SYMBOL = '{symbol}'
        ORDER BY TRADE_DATE;
    """
    cursor.execute(query)
    data = cursor.fetchall()

    columns = ["TRADE_DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "MA10", "MA50", "SIGNAL"]
    df = pd.DataFrame(data, columns=columns)
    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"])

    cursor.close()
    conn.close()

    return df

st.title("Stock Market Dashboard")

if "df" not in st.session_state:
    st.session_state.df = None

symbol = st.text_input("Enter Stock Symbol (e.g., AAPL, TSLA, MSFT):").upper()

if st.button("Fetch and Analyze"):
    if symbol:
        try:
            st.info(f"Fetching data for {symbol}...")
            df = fetch_data_from_alpha_vantage(symbol)
            insert_new_data_into_snowflake(df, symbol)

            st.info("Fetching data from Snowflake...")
            df = fetch_data_from_snowflake(symbol)

            if not df.empty:
                st.session_state.df = df 
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=df["TRADE_DATE"], y=df["CLOSE"], mode="lines", name="Closing Price"))
                fig.add_trace(go.Scatter(x=df["TRADE_DATE"], y=df["MA10"], mode="lines", name="MA10", line=dict(dash="dot")))
                fig.add_trace(go.Scatter(x=df["TRADE_DATE"], y=df["MA50"], mode="lines", name="MA50", line=dict(dash="dot")))
                st.plotly_chart(fig)

                st.subheader(f"Data Table for {symbol}")
                st.dataframe(df)
            else:
                st.warning(f"No data available for {symbol} in the Snowflake table.")
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
    else:
        st.warning("Please enter a stock symbol.")

if st.button("Predict Next 10 Days"):
    if st.session_state.df is not None:
        try:
            st.info(f"Training model and predicting next 10 days for {symbol}...")
            predictions = train_predict_stock_model(st.session_state.df)

            future_dates = pd.date_range(start=st.session_state.df["TRADE_DATE"].iloc[-1], periods=10, freq="B") 
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=st.session_state.df["TRADE_DATE"], y=st.session_state.df["CLOSE"], mode="lines", name="Closing Price"))
            fig.add_trace(go.Scatter(x=future_dates, y=predictions, mode="lines", name="Predicted Prices", line=dict(dash="dot", color="orange")))
            st.plotly_chart(fig)

            st.subheader("Predicted Prices for the Next 10 Days")
            prediction_df = pd.DataFrame({"Date": future_dates, "Predicted Price": predictions})
            st.dataframe(prediction_df)
        except Exception as e:
            st.error(f"An error occurred during prediction: {str(e)}")
    else:
        st.warning("Please fetch and analyze the data first.")
