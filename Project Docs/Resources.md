# Financial Market ETL — Data Resources

# Part 1: Twelve Data

The financial market ETL pipeline will use **Twelve Data** as its primary data provider for retrieving daily historical stock prices and the latest stock price.

- **Provider:** [Twelve Data](https://twelvedata.com/)
- **API documentation:** [Twelve Data API Documentation](https://twelvedata.com/docs)
- **API call limits:** [Twelve Data API Call Limits](https://support.twelvedata.com/en/articles/5194820-api-call-limits)
- **Plan:** Basic (Free)

## 2. API Usage Limits

The Basic plan provides the following API credit limits:

| Limit | Value |
|---|---:|
| Daily credits | 800 |
| Credits per minute | 8 |

## 3. API Endpoints

### A. Daily Historical Prices

`https://api.twelvedata.com/time_series?symbol=AAPL&interval=1day&apikey=YOUR_API_KEY`

Returns daily OHLCV data:

| Field | Description |
|---|---|
| `meta` | Symbol, exchange, currency, timezone, and instrument type |
| `values[].datetime` | Trading date |
| `values[].open` | Opening price |
| `values[].high` | Daily high |
| `values[].low` | Daily low |
| `values[].close` | Closing price |
| `values[].volume` | Trading volume |
| `status` | Validate API success or handle errors |

**Target tables:** `dim_symbol` (metadata) and `fact_daily_prices` (OHLCV).

## Example: Twelve Data `time_series` Response

```json
{
  "meta": {
    "symbol": "AAPL",
    "interval": "1day",
    "currency": "USD",
    "exchange_timezone": "America/New_York",
    "exchange": "NASDAQ",
    "mic_code": "XNGS",
    "type": "Common Stock"
  },
  "values": [
    {
      "datetime": "2026-10-09",
      "open": "254.50",
      "high": "258.30",
      "low": "253.20",
      "close": "257.80",
      "volume": "45678900"
    },
    {
      "datetime": "2026-10-08",
      "open": "251.10",
      "high": "255.60",
      "low": "250.40",
      "close": "254.20",
      "volume": "38901200"
    }
  ],
  "status": "ok"
}
```

### B. Latest Price

`https://api.twelvedata.com/price?symbol=AAPL&apikey=YOUR_API_KEY`

Example response:

`{"price": "200.99001"}`

Returns the latest available price, which may differ from the daily closing price.

## 4. Critical ETL Rules

1. **Data types:** Convert numeric strings to appropriate numeric types and `datetime` to a date.
2. **Ordering:** Sort historical records oldest to newest before calculating returns or moving averages.
3. **Incomplete data:** The current trading day's OHLCV may be preliminary. Flag it or process finalized daily data after market close.
4. **Symbol mapping:** Historical rows may not contain the symbol; associate each row with the symbol from `meta` or the request.
5. **Precision:** Preserve calculation precision and apply consistent database decimal types.
6. **Upserts:** Use `(symbol_id, trading_date)` as the daily price key to prevent duplicates and update revised records.
7. **Error handling:** Validate API errors even when HTTP returns `200`; implement throttling, retries, and logging.
8. **Security:** Store the API key in an environment variable or secrets manager, never in source control.

## 5. Main Analytical Outputs

- Daily percentage returns.
- Rolling moving averages.
- Daily trading-volume statistics.
- Latest stock price monitoring.

**Important:** Verify endpoint behavior and credit costs against the official documentation before implementation.

---

## Twelve Data — API Parameters

### Required Parameters

- `symbol` (string): The ticker symbol or instrument name, e.g. `AAPL`, `EUR/USD`, or `BTC/USD`. Multiple symbols may be supported as a comma-separated list, e.g. `AAPL,MSFT`, depending on the endpoint and plan but its symbol count as a request.
- `interval` (string): The data granularity. Supported values include `1min`, `5min`, `15min`, `30min`, `45min`, `1h`, `2h`, `4h`, `1day`, `1week`, and `1month`.
- `apikey` (string): Your Twelve Data API key. It can also be provided using the `Authorization: apikey YOUR_API_KEY` header.

### Optional Parameters

- `outputsize` (integer): Number of data points to retrieve. Default is `30`; maximum is `5000` per request, subject to endpoint and plan restrictions.
- `start_date` (string): Start date/time, formatted as `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS`.
- `end_date` (string): End date/time for historical data.
- `date` (string): Retrieve data for a specific date or supported relative keyword, such as `today`, `yesterday`, or `YYYY-MM-DD`.
- `format` (string): Response format: `JSON` (default) or `CSV`.
- `timezone` (string): Timezone for timestamps, such as `Exchange` (default), `UTC`, or an IANA timezone like `America/New_York`. Applies to intraday intervals shorter than one day.
- `exchange` (string): Exchange name or MIC code to identify a specific listing when a symbol is ambiguous.
- `order` (string): Sort order, either `ASC` (oldest first) or `DESC` (newest first).

**Official documentation:** [Twelve Data API Documentation](https://twelvedata.com/docs)

---

