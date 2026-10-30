# Most Important Glossary for the project

- ETL : Extract / Transform / Load for the data.
- OHLCV : Open / High / Low / Close / Vloume => 5 daily numbers per Stock.
- Symbol / ticker : Stock code (Apple => AAPL).
- Provider : Data Company (Twelve Data / EODHD).
- Backfill : ONE-TIME load historical Data.
- Increamental Extraction : Fetch Only what's new since the last time (DAILY)
- Watermark : a mark for a symbol (Last date I loaded data for this symbol)
- Rate Limit : Max Calls the API Allows
- Credit : API currency -> one symbol = one request
- Batch : Group of requests sized to fit the per-minute credit limit
- Deferred : Symbol skipped for a day - it will go first in the next run
- Exponential Backoff : retry waits the double 1s , 2s , 4s , ... so we don't hammer a struggling server.
- HTTP 409 : too many requests
- HTTP 401/403 : Bad request / retry is pointless -> ex : API endpoint is wrong.
- PayLoad : Body of Response Actual JSON
- Quarantine : separate table for bad requests -> for inspection/monitoring
- Raw Layer : 	Storage of the original response, untouched, append-only (insert only, never update/delete)
- Landing Table : Temporary holding area (stg_api_responses) where `extract` drops responses for the next step `validate`
- Priliminary bar : Today’s candle before the market closes: numbers may still change
- Idempotent : is running something more than one time. we need No Duplicates
- Fail Fast : Crash immedietly if config is missing
- Cross-Check : fetching the same stock for a provider to verify the other

