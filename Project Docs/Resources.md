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

# Part 2: EODHD

The financial market ETL pipeline will use **EODHD** as its secondary data provider for retrieving daily end-of-day stock prices, including Egyptian Exchange (EGX) symbols, and for cross-checking Twelve Data.

- **Provider:** [EODHD](https://eodhd.com/)
- **API documentation:** [EODHD End-of-Day Historical Data API](https://eodhd.com/financial-apis/api-for-historical-data-and-volumes)
- **Supported exchanges:** [EODHD List of Stock Markets](https://eodhd.com/list-of-stock-markets)
- **Plan:** Free

## 2. API Usage Limits

The Free plan provides the following limits:

| Limit | Value |
|---|---:|
| Daily API calls | 20 |
| History depth | 1 year |
| Data type | End-of-day |

## 3. API Endpoints

### A. Daily Historical Prices

`https://eodhd.com/api/eod/AAPL.US?api_token=YOUR_API_TOKEN&from=2026-10-05&to=2026-10-09&period=d&fmt=json`

Returns a JSON array with one object per trading day (daily OHLCV data):

| Field | Description |
|---|---|
| `date` | Trading date |
| `open` | Opening price |
| `high` | Daily high |
| `low` | Daily low |
| `close` | Closing price |
| `adjusted_close` | Close adjusted for splits and dividends |
| `volume` | Trading volume |

The response has no metadata block and no status field.

**Target tables:** `fact_daily_prices` (OHLCV). `dim_symbol` details (name, exchange, currency) come from configuration, since the response does not include them.

## Example: EODHD `eod` Response

Values are illustrative.

```json
[
  {
    "date": "2026-10-08",
    "open": 251.10,
    "high": 255.60,
    "low": 250.40,
    "close": 254.20,
    "adjusted_close": 254.20,
    "volume": 38901200
  },
  {
    "date": "2026-10-09",
    "open": 254.50,
    "high": 258.30,
    "low": 253.20,
    "close": 257.80,
    "adjusted_close": 257.80,
    "volume": 45678900
  }
]
```

### B. Latest Price

`https://eodhd.com/api/real-time/AAPL.US?api_token=YOUR_API_TOKEN&fmt=json`

Returns the latest available quote (price, volume, previous close, and change), which may differ from the daily closing price. Verify availability, delay, and call cost on the Free plan before using this endpoint.

### C. Egyptian Exchange (EGX) Example

`https://eodhd.com/api/eod/COMI.EGX?api_token=YOUR_API_TOKEN&from=2026-10-05&to=2026-10-09&period=d&fmt=json`

Ticker format is `SYMBOL.EXCHANGE`, for example `AAPL.US` and `COMI.EGX`. Confirm each ticker on the supported exchanges page.

## 4. Critical ETL Rules

1. **Data types:** Numbers arrive as JSON numbers. Convert `date` to a date and `volume` to an integer.
2. **Ordering:** Sort historical records oldest to newest before calculating returns or moving averages.
3. **Incomplete data:** End-of-day bars appear only after the market closes. An empty result for today is not necessarily an error. Process finalized daily data after market close.
4. **Symbol mapping:** Rows do not contain the symbol; associate each row with the symbol from the request.
5. **Precision:** Preserve calculation precision and apply consistent database decimal types. Store both `close` and `adjusted_close`, and use one consistently for returns.
6. **Upserts:** Use `(symbol_id, trading_date)` as the daily price key to prevent duplicates and update revised records.
7. **Error handling:** An empty array `[]` can mean a reversed date range (`from` later than `to`), a weekend or holiday, an unknown symbol, or an unpublished bar. Validate the payload shape, and implement throttling, retries, and logging. Track the 20 calls per day budget.
8. **Security:** Store the API token in an environment variable or secrets manager, never in source control.

## 5. Main Analytical Outputs

- Daily percentage returns.
- Rolling moving averages.
- Daily trading-volume statistics.
- EGX symbol coverage.
- Cross-check of Twelve Data values (tested for AAPL: OHLCV matched).

**Important:** Verify endpoint behavior and call limits against the official documentation before implementation.

---

## EODHD — API Parameters

### Required Parameters

- `symbol` (string, in the URL path): The ticker and exchange code in the format `SYMBOL.EXCHANGE`, e.g. `AAPL.US` or `COMI.EGX`.
- `api_token` (string): Your EODHD API token.

### Optional Parameters

- `fmt` (string): Response format: `json` or `csv`. Use `json`; CSV may be the default if omitted.
- `period` (string): Data granularity: `d` (daily), `w` (weekly), or `m` (monthly). Default is `d`.
- `from` (string): Start date, formatted as `YYYY-MM-DD`.
- `to` (string): End date, formatted as `YYYY-MM-DD`. Must not be earlier than `from`.
- `order` (string): Sort order, either `a` (ascending, oldest first) or `d` (descending, newest first). Default is ascending.
- `filter` (string): Return a single value instead of the full table, such as `last_close` or `last_volume`.

**Official documentation:** [EODHD End-of-Day Historical Data API](https://eodhd.com/financial-apis/api-for-historical-data-and-volumes)

---