import yfinance as yf

ticker_symbol = "NVDA"

# Define the start and end dates
start_date = "2022-01-01"
end_date = "2025-06-01"

# Download the data
data = yf.download(ticker_symbol, start=start_date, end=end_date)

data.to_csv("./yfinance-data/"+ ticker_symbol + ".csv")