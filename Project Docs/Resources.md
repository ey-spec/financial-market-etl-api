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

# Part 2: Finnhub

## 1. Data Source

The financial market ETL pipeline will use **Finnhub** to retrieve historical stock prices.

- **Provider:** [Finnhub](https://finnhub.io/)
- **API documentation:** [Stock Candles API](https://finnhub.io/docs/api/stock-candles)
- **Plan:** Free — $0/month

## 2. API Usage Limits

| Limit | Value |
|---|---|
| API calls per minute | 60 |
| Monthly cost | $0 |
| License | Personal use |

**Pricing reference:** [Finnhub Pricing](https://finnhub.io/pricing)

The pipeline must respect the rate limit, track API calls, and handle throttling errors with retries and backoff.

## 3. API Endpoints

### A. Daily Historical Prices

**Endpoint:**

`https://finnhub.io/api/v1/stock/candle?symbol=AAPL&resolution=D&from=START_TIMESTAMP&to=END_TIMESTAMP&token=YOUR_API_KEY`

| Parameter | Description |
|---|---|
| `symbol` | Stock ticker, e.g. `AAPL` |
| `resolution` | `D` for daily candles |
| `from` | Start time as a Unix timestamp |
| `to` | End time as a Unix timestamp |
| `token` | Finnhub API token |

Returns daily OHLCV data in parallel arrays.

| Field | Description |
|---|---|
| `o[]` | Opening prices |
| `h[]` | Daily high prices |
| `l[]` | Daily low prices |
| `c[]` | Closing prices |
| `v[]` | Trading volumes |
| `t[]` | Unix timestamps |
| `s` | Response status, such as `ok` or `no_data` |

**Target table:** `fact_daily_prices`

### Example: `stock/candle` Response

```json
{
  "c": [257.80, 254.20],
  "h": [258.30, 255.60],
  "l": [253.20, 250.40],
  "o": [254.50, 251.10],
  "s": "ok",
  "t": [1791504000, 1791417600],
  "v": [45678900, 38901200]
}
```

*Illustrative example only; values are not verified live market data.*

Each array index represents one trading day. Values at matching indexes belong to the same candle.

## 4. Critical ETL Rules

1. **Data types:** Convert price arrays to decimal values, volume to an integer type, and timestamps to dates.
2. **Array mapping:** Combine values at matching indexes into individual daily records.
3. **Ordering:** Sort records oldest to newest before calculating returns or moving averages.
4. **Missing data:** Handle `no_data`, missing fields, and incomplete candles.
5. **Symbol mapping:** Add the requested symbol to each record because it is not included in the response.
6. **Upserts:** Use `(symbol, trading_date)` as the daily price key to prevent duplicates and update revised records.
7. **Error handling:** Validate response status and handle API errors, throttling, retries, and logging.
8. **Security:** Store the API token in an environment variable or secrets manager, never in source control.

## 5. Main Analytical Outputs

- Daily percentage returns.
- Rolling moving averages.
- Daily trading-volume statistics.
- Historical stock price analysis.

**Official references:**
- [Finnhub Stock Candles API](https://finnhub.io/docs/api/stock-candles)
- [Finnhub Pricing](https://finnhub.io/pricing)
- [Finnhub Terms of Service](https://finnhub.io/terms-of-service)