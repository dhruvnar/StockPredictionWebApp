import os

# Snowflake credentials
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "username")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "password")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "account_identifier")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "database")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "schema")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "warehouse")

# Alpha Vantage API Key
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY", "api_key")