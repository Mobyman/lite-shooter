# TON Lite Server Load Runner

Minimal Go runner for TON Lite Server load testing with HTML/CSV reports.

## Build

```bash
docker build -t ls-load .
```

## Docker Compose

`docker-compose.yml` runs a comparison between FirstLS and SecondLS configs.
Place `global-config.json` in the project root.

```bash
docker compose run --rm load
```

## Run

```bash
docker run --rm -v "$PWD":/data ls-load \
  --configs /data/config.json \
  --accounts /data/accounts.txt \
  --blocks last:200 \
  --concurrency 5,10,20,50 \
  --out /data/results
```

Results are written to `results/<timestamp>/` with:
- `report.html`
- `summary.csv`
- `summary.json`

When `--duration` is set, the report includes time-series charts (RPS/sec, Errors/sec, and latency percentiles over time).

To regenerate a report without rerunning a test:

```bash
./ls-load --report-from results/20260130-150157
```

## .env support

If a `.env` file exists in the working directory, it is loaded automatically.
Flags always override `.env` values.

Supported variables:
- `LS_LOAD_CONFIGS` (comma-separated, optional alias: `name=path`)
- `LS_LOAD_CONCURRENCY` (comma-separated levels)
- `LS_LOAD_STEPS` (comma-separated step levels; overrides concurrency)
- `LS_LOAD_STEP_DURATION` (duration per step, e.g. `5m`)
- `LS_LOAD_BLOCKS` (`last:N` or `range:FROM-TO`)
- `LS_LOAD_BLOCKS_RANDOM` (true/false; randomize block selection per request)
- `LS_LOAD_BLOCKS_REFRESH` (refresh interval for `last:N` when random enabled, e.g. `5s`)
- `LS_LOAD_ACCOUNTS` (path to accounts file)
- `LS_LOAD_ACCOUNTS_COUNT` (default random accounts count)
- `LS_LOAD_ACCOUNTS_WARMUP_BLOCKS` (masterchain blocks to scan during warmup)
- `LS_LOAD_ACCOUNTS_SHUFFLE` (true/false; shuffle accounts on load)
- `LS_LOAD_OUT` (output directory)
- `LS_LOAD_TIMEOUT` (per-request timeout, e.g. `10s`)
- `LS_LOAD_DURATION` (test duration per scenario, e.g. `10s`)
- `LS_LOAD_REQUEST_LOG` (per-request JSONL log path; use `auto` for results dir, `off` to disable)
- `LS_LOAD_REPORT_FROM` (regenerate report from existing results dir)
- `LS_LOAD_REPORT_MAX_POINTS` (max points per series in report; `0` = no downsample)
- `LS_LOAD_MAX_CONNECTIONS` (max connections to liteservers, `0` = auto)
- `LS_LOAD_WORKERS_PER_CONN` (workers per connection, `0` = default)
- `LS_LOAD_POOL_STRATEGY` (`best-ping` or `first-working`)
- `LS_LOAD_RETRIES` (retry attempts, `0` = auto)
- `LS_LOAD_PROOF` (`unsafe|fast|secure`)
- `LS_LOAD_ENV` (optional path to .env file)

Example `.env` is in `.env.example`.

Defaults (not configurable via env):
- Mode is fixed to `both`.
- Aggressive mode is enabled.
- Accounts are generated when `--accounts` is not set.
- Accounts warmup is enabled (scan recent shard blocks to find existing accounts).
- Account selection is randomized per request.

## Inputs

- `--configs` accepts comma-separated paths or globs.
- `--accounts` is a text file with one address per line. Lines starting with `#` are ignored.
- `--blocks` accepts `last:N` or `range:FROM-TO` (masterchain seqno).

## Flags

- `--concurrency`: comma-separated levels (default: `5,10,20,50`)
- `--steps`: comma-separated step levels; overrides `--concurrency`
- `--step-duration`: duration per step (e.g. `5m`)
- `--timeout`: per-request timeout (default: `10s`)
- `--duration`: test duration per scenario (e.g. `10s`)
- `--request-log`: per-request JSONL log path (`auto` = results dir, `off` = disable)
- `--report-from`: regenerate `report.html` from existing results dir
- `--report-max-points`: max points per series in HTML report (`0` = no downsample)
- `--max-connections`: max connections to liteservers (`0` = auto)
- `--workers-per-conn`: workers per connection (`0` = default)
- `--pool-strategy`: `best-ping` or `first-working`
- `--blocks-random`: randomize block selection per request (reduces caching)
- `--blocks-refresh`: refresh interval for `last:N` when random enabled (default: `5s`)
- `--accounts-count`: number of random accounts (default: 10000)
- `--accounts-warmup`: scan recent shard blocks to collect existing accounts
- `--accounts-warmup-blocks`: masterchain blocks to scan during warmup (default: 8)
- `--accounts-shuffle`: shuffle accounts after load
- `--retries`: LiteServer retry attempts (default: `0` = auto)
- `--proof`: `unsafe`, `fast`, `secure` (default: `fast`)

## Example config

```json
{
  "liteservers": [
    {
      "ip": 12345678,
      "port": 12345,
      "id": {
        "@type": "pub.ed25519",
        "key": "S0VZ="
      }
    }
  ]
}
```
