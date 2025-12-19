
# 1. Interest Rates
| Country | Source              | Notes              |
| ------- | ------------------- | ------------------ |
| USA     | FRED API            | Federal funds rate |
| UK      | Bank of England API | Bank rate          |
| EU      | ECB Data API        | Refi rate          |
| Japan   | BOJ Statistics      | Policy rate        |


##################################
# 2. Spot Exchange Rates
| Source            | Data                            | Free?              |
| ----------------- | ------------------------------- | ------------------ |
| **Alpha Vantage** | Real-time + historical FX rates | Free but limited   |
| **TwelveData**    | FX data + technical analysis    | Free (small), paid |
| **Oanda API**     | Professional FX data            | Paid               |

3.
| Category            | Required Data            | Where to Get It   | Storage    Extraction   

| **Stocks (Graham)** | EPS, Growth, Bond yield  | yfinance, Alpha Vantage, FRED   | PostgreSQL | Python (yfinance, requests) |
| **Forex**           | Rates, interest rates, inflation  | Alpha Vantage, TwelveData, FRED | PostgreSQL | Python API calls        |
| **Commodities**     | Spot, futures, interest rates, storage, convenience yield | Yahoo Finance, Quandl, EIA      | PostgreSQL | Python API calls            |
