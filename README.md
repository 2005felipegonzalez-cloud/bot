# Polymarket US Fast-Move Alert Bot + Dashboard

Watches **every active market on Polymarket US** (`polymarket.us` — the
CFTC-regulated, US-only exchange, *not* the international `polymarket.com`
site) and pings you via an [ntfy.sh](https://ntfy.sh) push notification when
a market's price moves unusually fast. Also runs a local web dashboard so
you can see the current top movers — name, price, size of the move, and a
link to the market — at a glance. Uses Polymarket US's public market-data
API — no API key or wallet required, and ntfy needs no account either.

> Polymarket US is a separate product/company from the original
> polymarket.com (it's the CFTC-approved, intermediated exchange that opened
> to US users via iOS/Android apps and polymarket.us in late 2025/2026). It
> has its own API at `gateway.polymarket.us`, its own market catalog, and —
> as of this writing — no public volume data, so this bot only detects fast
> **price** moves, not volume spikes.

## How it decides something is "moving fast"

Every poll cycle (default: every 60 seconds) it compares each market's current
price to what it was ~5 minutes ago:

- **Price alert** — probability moved more than `PRICE_MOVE_THRESHOLD` (default
  0.06 = 6 percentage points) in that window, in either direction.

Each market has its own 30-minute cooldown per alert type, so a market that
keeps moving won't spam you every cycle.

All thresholds live in `.env` — see the tuning notes below.

## The dashboard

Once the bot is running, open **http://127.0.0.1:8787** in a browser. It
shows every market with enough history to measure a move, ranked by size of
move (largest first), auto-refreshing every 5 seconds. Each row has a
"open" link straight to that market on polymarket.us. Use the search box to
filter by name. `DASHBOARD_PORT`/`DASHBOARD_HOST` in `.env` control where it
listens (defaults to localhost-only).

## 1. Install

```bash
pip install -r requirements.txt
cp .env.example .env
```

Then open `.env` and fill in real values (see the next two sections).

## 2. Set up ntfy

[ntfy.sh](https://ntfy.sh) is a free push-notification service with no
account or API key — you just publish to a "topic" (like a channel name)
and subscribe to that same topic to receive alerts.

1. Pick a hard-to-guess topic name, e.g. `polymarket-alerts-a1b2c3` — topic
   names on the public `ntfy.sh` server aren't secret or access-controlled,
   so anyone who knows/guesses the name can read your alerts and anyone can
   publish to it.
2. Subscribe to it one of these ways:
   - **Phone/desktop app**: install [ntfy](https://ntfy.sh/#subscribe) and
     add your topic.
   - **Browser**: open `https://ntfy.sh/<your-topic>` and click "Subscribe".
3. In `.env`, set `NTFY_TOPIC=` to your topic name (leave `NTFY_SERVER` as
   `https://ntfy.sh` unless you're self-hosting an ntfy server).

Send yourself a test message any time with:
```bash
curl -d "test" https://ntfy.sh/<your-topic>
```

## 4. Run it

```bash
python3 polymarket_alert_bot.py
```

You'll see log lines each cycle like `Fetched 4200 active markets / Checked 4200
markets this cycle`, and an alert log line whenever it fires.

### Keeping it running 24/7

This is a long-running loop, not a one-shot script, so cron isn't the right
tool. Pick one:

- **Simplest (a machine you leave on):**
  ```bash
  nohup python3 polymarket_alert_bot.py > bot.log 2>&1 &
  ```
- **tmux/screen** so you can reattach and watch logs live.
- **systemd** (Linux server) — a small unit file that restarts it if it crashes:
  ```ini
  [Unit]
  Description=Polymarket Alert Bot
  After=network.target

  [Service]
  WorkingDirectory=/path/to/this/folder
  ExecStart=/usr/bin/python3 polymarket_alert_bot.py
  Restart=always
  EnvironmentFile=/path/to/this/folder/.env

  [Install]
  WantedBy=multi-user.target
  ```
  Then `systemctl enable --now polymarket-bot`.
- **A small always-on VPS** (DigitalOcean/Linode/etc, a few dollars a month) if
  you don't want to leave your own computer on.

## Tuning

Edit these in `.env`:

| Variable | What it does |
|---|---|
| `POLL_INTERVAL_SECONDS` | How often to check all markets (default 60s) |
| `LOOKBACK_MINUTES` | The window used to measure "fast" (default 5 min) |
| `PRICE_MOVE_THRESHOLD` | Probability-point move that triggers an alert (0.06 = 6 points) |
| `COOLDOWN_MINUTES` | Minimum time between repeat alerts on the same market |
| `SPORTS_FILTER` | Comma-separated sports to track: `ufc`, `nfl`, `soccer`, `mlb`, `cfb` (default: all five). Polymarket US has no "sport" field on a market, only a per-team league code, so `soccer` is inferred as "any team league that isn't one of the other four or a known non-soccer sport" — see `market_sport()` in the script if a league is misclassified. |
| `DASHBOARD_HOST` / `DASHBOARD_PORT` | Where the local top-movers dashboard listens (default `127.0.0.1:8787`) |
| `DASHBOARD_TOP_N` | Max number of rows kept in the dashboard's top-movers list (default 150) |

If you're getting too many alerts, raise the thresholds or raise
`MIN_LIQUIDITY`. If you're missing moves, lower `LOOKBACK_MINUTES` or the
thresholds.

## Notes / limitations

- History is kept in memory only — if you restart the bot, it needs one
  `LOOKBACK_MINUTES` cycle to "warm up" again before it can detect moves
  (the dashboard will show no rows until then).
- The "price" tracked is the first outcome's price (usually "Yes"). For
  multi-outcome events (e.g. "Who will win the game?"), each side is its own
  market in Polymarket US's API, so they're each tracked individually.
- No volume data is available from Polymarket US's public API yet, so
  there's no volume-spike alert or liquidity filter (unlike the old
  polymarket.com-based version of this bot).
- The market link (`polymarket.us/event/<slug>`) is built from the market's
  slug, same pattern used by the site itself.
- Polymarket US is a new, sports-first product still expanding its market
  catalog (politics, economics, etc. are rolling out gradually), so the
  total number of markets you see may be smaller than the international
  polymarket.com site.
