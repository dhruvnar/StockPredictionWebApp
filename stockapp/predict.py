import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error


def create_features(data, lags=10):
    """
    Add lag features and moving averages to the dataset.
    """
    for lag in range(1, lags + 1):
        data[f"lag_{lag}"] = data["CLOSE"].shift(lag)
    data["MA10"] = data["CLOSE"].rolling(window=10).mean()
    data["MA50"] = data["CLOSE"].rolling(window=50).mean()
    return data


def train_predict_stock_model(data):
    """
    Train a regression model and predict the next 10 days of stock prices.
    """
    data = create_features(data)
    data = data.dropna()

    features = [col for col in data.columns if col.startswith("lag_") or "MA" in col]
    target = "CLOSE"

    X = data[features]
    y = data[target]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = XGBRegressor(n_estimators=100, learning_rate=0.1, max_depth=5, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    print(f"Test RMSE: {np.sqrt(mean_squared_error(y_test, y_pred))}")

    last_row = data.iloc[-1][features].values.reshape(1, -1)
    predictions = []
    for _ in range(10):
        pred = model.predict(last_row)[0]
        predictions.append(pred)
        last_row = np.append(last_row[:, 1:], pred).reshape(1, -1)

    return predictions
