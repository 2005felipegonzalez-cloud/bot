#!/usr/bin/env python3
"""
Polymarket US Fast-Move Alert Bot + Dashboard
----------------------------------------------
Polls ALL active markets on Polymarket US (the CFTC-regulated, US-only
exchange at polymarket.us - not the international polymarket.com site) via
its public market-data API and sends an ntfy.sh push notification whenever a
market's price moves unusually fast. Also runs a local web dashboard so you
can see the top movers live: name, price, size of the move, and a link to
find the market.

No API key / wallet needed - Polymarket US's market-data endpoints are
public and unauthenticated (trading endpoints require auth, but we don't
trade here). ntfy.sh also needs no account/API key - just a topic name.

Note: Polymarket US's public markets API does not currently expose volume
data (it's a newer, sports-first product), so unlike the old international
version of this bot, detection here is price-move only.

Setup:
    pip install -r requirements.txt
    cp .env.example .env      # then fill in your values
    python3 polymarket_alert_bot.py

Then open http://127.0.0.1:8787 in a browser for the live top-movers
dashboard.

See README.md for how to pick an ntfy topic and subscribe to it, and how to
keep this running 24/7.
"""

import os
import json
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration (edit via .env - see .env.example)
# ---------------------------------------------------------------------------
# Polymarket US - the CFTC-regulated, US-only exchange (polymarket.us).
# This is a different product/company entity than the international
# polymarket.com site, with its own separate API and market catalog.
US_MARKETS_API = "https://gateway.polymarket.us/v1/markets"
US_EVENT_URL = "https://polymarket.us/event/{slug}"

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
LOOKBACK_MINUTES = float(os.getenv("LOOKBACK_MINUTES", "5"))
PRICE_MOVE_THRESHOLD = float(os.getenv("PRICE_MOVE_THRESHOLD", "0.06"))  # 0.06 = 6 points of probability
COOLDOWN_MINUTES = float(os.getenv("COOLDOWN_MINUTES", "30"))
HISTORY_RETENTION_MINUTES = max(LOOKBACK_MINUTES * 3, 60)

DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "127.0.0.1")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8787"))
DASHBOARD_TOP_N = int(os.getenv("DASHBOARD_TOP_N", "150"))

NTFY_SERVER = os.getenv("NTFY_SERVER", "https://ntfy.sh").rstrip("/")
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "")
# ntfy's JSON publish API wants priority as a number (1-5), not a name.
NTFY_PRIORITY_NAMES = {"min": 1, "low": 2, "default": 3, "high": 4, "max": 5, "urgent": 5}
NTFY_PRIORITY = NTFY_PRIORITY_NAMES.get(
    os.getenv("NTFY_PRIORITY", "default").strip().lower(), 3
)

SPORTS_FILTER = {
    s.strip().lower()
    for s in os.getenv("SPORTS_FILTER", "ufc,nfl,soccer,mlb,cfb").split(",")
    if s.strip()
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("polymarket-us-bot")

# Polymarket US's markets API has no "sport" field, only a per-team "league"
# code (nfl, cfb, mlb, epl, lal, cs2, atp, ...) - surveyed across all ~89k
# active markets on 2026-09-08. We map the leagues that clearly belong to a
# sport OTHER than soccer here, and treat every remaining team-based league
# as soccer, since that "everything else" bucket is dozens of countries'
# domestic soccer leagues/cups and would be impractical to enumerate fully.
LEAGUE_SPORT_OVERRIDES = {
    "nfl": "nfl",
    "cfb": "cfb",
    "mlb": "mlb",
    "ufc": "ufc",
    "dwcs": "ufc",  # Dana White's Contender Series - UFC's own prospect show
}
NON_SOCCER_LEAGUES = {
    "cs2", "r6", "valorant", "dota2", "lol",  # esports
    "atp", "wta", "itfme", "itfwo", "atpdb", "wtt",  # tennis / table tennis
    "setkameua", "setkamemd", "setkamecz", "setkawoua",  # Setka Cup (table tennis)
    "pdc", "modus",  # darts
    "county", "cplcr", "testcr",  # cricket
    "boxing", "cbb", "nba", "wnba", "nhl", "npb", "kbo",  # boxing/basketball/hockey/non-MLB baseball
}


def market_sport(m):
    """Best-effort sport classification from a raw market's team leagues.
    Returns None if the market has no team/league info (e.g. politics)."""
    for side in m.get("marketSides", []):
        team = side.get("team") or {}
        league = (team.get("league") or "").lower()
        if not league:
            continue
        if league in LEAGUE_SPORT_OVERRIDES:
            return LEAGUE_SPORT_OVERRIDES[league]
        if league not in NON_SOCCER_LEAGUES:
            return "soccer"
    return None

# market_id -> deque of (timestamp, price)
history: dict = defaultdict(deque)
# f"{market_id}:{kind}" -> last alert unix timestamp
last_alert: dict = {}
last_alert_lock = threading.Lock()
# Alerts do blocking network I/O (ntfy webhook); send them off-thread so a
# burst of correlated alerts (e.g. many markets moving on the same news)
# can't stall the per-cycle market scan and the dashboard update.
alert_pool = ThreadPoolExecutor(max_workers=4)

# Latest computed top-movers snapshot, shared with the dashboard HTTP server.
dashboard_lock = threading.Lock()
dashboard_state = {
    "generated_at": None,
    "markets_tracked": 0,
    "movers": [],
    "sports_filter": sorted(SPORTS_FILTER),
}


PAGE_LIMIT = 500  # server caps each page at 500 regardless of a higher requested limit
PAGE_CONCURRENCY = 8  # Polymarket US's rate limits aren't publicly documented; keep this modest


def fetch_page(offset):
    params = {"limit": PAGE_LIMIT, "offset": offset, "active": "true", "closed": "false"}
    try:
        resp = requests.get(US_MARKETS_API, params=params, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.warning("Fetch failed at offset %s: %s", offset, e)
        return []
    return resp.json().get("markets", [])


def fetch_all_active_markets():
    """Paginate through the Polymarket US markets API and return every active,
    open market. Polymarket US currently lists tens of thousands of markets
    (mostly far-future sports futures), so pages are fetched several at a
    time to keep a full cycle from taking minutes."""
    markets = []
    offset = 0
    with ThreadPoolExecutor(max_workers=PAGE_CONCURRENCY) as pool:
        while True:
            wave_offsets = [offset + i * PAGE_LIMIT for i in range(PAGE_CONCURRENCY)]
            batches = list(pool.map(fetch_page, wave_offsets))
            hit_end = False
            for batch in batches:
                markets.extend(batch)
                if len(batch) < PAGE_LIMIT:
                    hit_end = True
                    break
            if hit_end:
                break
            offset += PAGE_CONCURRENCY * PAGE_LIMIT
    return markets


def parse_market(m, sport):
    """Pull out the fields we need. Returns None if the market can't be read."""
    try:
        outcomes = json.loads(m.get("outcomes", "[]"))
        prices = json.loads(m.get("outcomePrices", "[]"))
    except (json.JSONDecodeError, TypeError):
        return None
    if not prices:
        return None
    try:
        price = float(prices[0])
    except (ValueError, IndexError):
        return None
    slug = m.get("slug", "")
    sides = m.get("marketSides") or []
    team = (sides[0].get("team") if sides else None) or {}
    return {
        "id": m.get("id"),
        "sport": sport,
        "question": m.get("question") or m.get("title") or "Unknown market",
        "slug": slug,
        "url": US_EVENT_URL.format(slug=slug) if slug else "",
        "outcome_label": outcomes[0] if outcomes else "Yes",
        "price": price,
        "icon": team.get("logo") or "",
    }


def send_ntfy(title, message, click_url=None, tags=None, icon=None):
    if not NTFY_TOPIC:
        log.debug("ntfy not configured, skipping.")
        return
    # JSON body (rather than X-Title/X-Tags headers) sidesteps header
    # encoding issues with non-ASCII characters in market questions.
    payload = {
        "topic": NTFY_TOPIC,
        "title": title,
        "message": message,
        "priority": NTFY_PRIORITY,
    }
    if tags:
        payload["tags"] = tags if isinstance(tags, list) else [tags]
    if click_url:
        payload["click"] = click_url
    if icon:
        payload["icon"] = icon
    try:
        r = requests.post(NTFY_SERVER, json=payload, timeout=10)
        r.raise_for_status()
        log.info("ntfy notification sent: %s", title)
    except requests.RequestException as e:
        log.error("ntfy send failed: %s", e)


def alert(kind, market, detail):
    key = f"{market['id']}:{kind}"
    now = time.time()
    with last_alert_lock:
        if now - last_alert.get(key, 0) < COOLDOWN_MINUTES * 60:
            return  # cooldown active, skip
        last_alert[key] = now

    url = market["url"] or "(no link available)"
    title = f"[{market['sport'].upper()}] {kind} - {market['question'][:80]}"
    body = (
        f"{market['question']}\n"
        f"{detail}\n"
        f"Current price ({market['outcome_label']}): {market['price']*100:.1f}%\n"
        f"Link: {url}\n"
        f"Time (UTC): {datetime.now(timezone.utc).isoformat(timespec='seconds')}"
    )
    log.info("ALERT [%s] %s", kind, market["question"])
    tags = "chart_with_upwards_trend" if "UP" in kind else "chart_with_downwards_trend"
    send_ntfy(title, body, click_url=market["url"] or None, tags=tags, icon=market.get("icon") or None)


def prune_history(dq, now):
    cutoff = now - HISTORY_RETENTION_MINUTES * 60
    while dq and dq[0][0] < cutoff:
        dq.popleft()


def find_reference(dq, now):
    """Return the snapshot closest to (now - LOOKBACK_MINUTES), or None if we don't have one yet."""
    target = now - LOOKBACK_MINUTES * 60
    ref = None
    for ts, price in dq:
        if ts <= target:
            ref = (ts, price)
        else:
            break
    return ref


def check_market(market, now):
    """Update history, fire an alert if this market moved fast enough, and
    return a mover dict (or None if there's not enough history yet)."""
    dq = history[market["id"]]
    ref = find_reference(dq, now)
    dq.append((now, market["price"]))
    prune_history(dq, now)

    if ref is None:
        return None  # not enough history yet for this market

    _, ref_price = ref
    if ref_price <= 0:
        return None  # market had no real starting quote yet (just went live) - not a real move

    price_delta = market["price"] - ref_price

    if abs(price_delta) >= PRICE_MOVE_THRESHOLD:
        direction = "UP" if price_delta > 0 else "DOWN"
        alert_pool.submit(
            alert,
            f"fast price move {direction}",
            market,
            f"Price moved {price_delta*100:+.1f} points in ~{LOOKBACK_MINUTES:.0f} min "
            f"({ref_price*100:.1f}% -> {market['price']*100:.1f}%)",
        )

    return {
        "id": market["id"],
        "sport": market["sport"],
        "question": market["question"],
        "outcome_label": market["outcome_label"],
        "url": market["url"],
        "price": market["price"],
        "ref_price": ref_price,
        "delta": price_delta,
        "direction": "UP" if price_delta > 0 else ("DOWN" if price_delta < 0 else "FLAT"),
    }


def run_once():
    now = time.time()
    raw_markets = fetch_all_active_markets()
    checked = 0
    movers = []
    for m in raw_markets:
        sport = market_sport(m)
        if sport not in SPORTS_FILTER:
            continue
        market = parse_market(m, sport)
        if market is None:
            continue
        mover = check_market(market, now)
        checked += 1
        if mover is not None:
            movers.append(mover)
    log.info(
        "Fetched %d active markets, %d match sports filter (%s)",
        len(raw_markets), checked, ", ".join(sorted(SPORTS_FILTER)),
    )

    movers.sort(key=lambda mv: abs(mv["delta"]), reverse=True)
    with dashboard_lock:
        dashboard_state["generated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        dashboard_state["markets_tracked"] = checked
        dashboard_state["movers"] = movers[:DASHBOARD_TOP_N]


# ---------------------------------------------------------------------------
# Dashboard (local web UI showing the current top movers)
# ---------------------------------------------------------------------------
DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Polymarket US - Top Movers</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root { color-scheme: dark; }
  body { margin: 0; background: #0b0d12; color: #e6e8eb; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
  header { padding: 16px 20px; border-bottom: 1px solid #22262e; position: sticky; top: 0; background: #0b0d12; z-index: 1; }
  h1 { font-size: 18px; margin: 0 0 6px 0; }
  #meta { color: #8a8f98; font-size: 12px; }
  #controls { padding: 10px 20px; display: flex; gap: 10px; align-items: center; }
  #search { flex: 1; max-width: 360px; background: #14161b; border: 1px solid #2a2e37; color: #e6e8eb; padding: 8px 10px; border-radius: 6px; font-size: 14px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { padding: 8px 14px; text-align: left; border-bottom: 1px solid #1c1f26; }
  th { color: #8a8f98; font-weight: 600; text-transform: uppercase; font-size: 11px; letter-spacing: .04em; position: sticky; top: 62px; background: #0b0d12; }
  tr:hover { background: #14161b; }
  a { color: #7ab8ff; text-decoration: none; }
  a:hover { text-decoration: underline; }
  .up { color: #37d67a; font-weight: 600; }
  .down { color: #ff5c6c; font-weight: 600; }
  .flat { color: #8a8f98; }
  .rank { color: #565c66; width: 34px; }
  .q { max-width: 480px; }
  .empty { padding: 40px 20px; color: #8a8f98; text-align: center; }
</style>
</head>
<body>
<header>
  <h1>Polymarket US - Top Movers</h1>
  <div id="meta">loading...</div>
</header>
<div id="controls">
  <input id="search" type="text" placeholder="Filter by market name...">
  <select id="sportFilter"><option value="">All sports</option></select>
</div>
<table>
  <thead>
    <tr>
      <th class="rank">#</th>
      <th>Sport</th>
      <th class="q">Market</th>
      <th>Side</th>
      <th>Price now</th>
      <th>Price before</th>
      <th>Move</th>
      <th>Link</th>
    </tr>
  </thead>
  <tbody id="rows"></tbody>
</table>
<div class="empty" id="empty" style="display:none;">
  No movers yet - the bot needs one lookback window to warm up after starting, or no market has moved past the threshold this cycle.
</div>

<script>
let allMovers = [];
let knownSports = new Set();

function pct(x) { return (x * 100).toFixed(1) + "%"; }
function pts(x) { return (x >= 0 ? "+" : "") + (x * 100).toFixed(1); }
function esc(s) {
  const d = document.createElement("div");
  d.textContent = s == null ? "" : String(s);
  return d.innerHTML;
}

function updateSportOptions() {
  const sel = document.getElementById("sportFilter");
  const current = sel.value;
  const opts = ["", ...Array.from(knownSports).sort()];
  sel.innerHTML = opts.map(s => `<option value="${esc(s)}">${s ? esc(s.toUpperCase()) : "All sports"}</option>`).join("");
  sel.value = current;
}

function render() {
  const q = document.getElementById("search").value.trim().toLowerCase();
  const sportVal = document.getElementById("sportFilter").value;
  let filtered = allMovers;
  if (sportVal) filtered = filtered.filter(m => m.sport === sportVal);
  if (q) filtered = filtered.filter(m => m.question.toLowerCase().includes(q));
  const rows = document.getElementById("rows");
  rows.innerHTML = "";
  document.getElementById("empty").style.display = filtered.length ? "none" : "block";
  filtered.forEach((m, i) => {
    const tr = document.createElement("tr");
    const dirClass = m.direction === "UP" ? "up" : (m.direction === "DOWN" ? "down" : "flat");
    const arrow = m.direction === "UP" ? "↑" : (m.direction === "DOWN" ? "↓" : "–");
    tr.innerHTML = `
      <td class="rank">${i + 1}</td>
      <td>${esc((m.sport || "").toUpperCase())}</td>
      <td class="q">${esc(m.question)}</td>
      <td>${esc(m.outcome_label)}</td>
      <td>${pct(m.price)}</td>
      <td>${pct(m.ref_price)}</td>
      <td class="${dirClass}">${arrow} ${pts(m.delta)} pts</td>
      <td>${m.url ? `<a href="${esc(m.url)}" target="_blank" rel="noopener">open</a>` : ""}</td>
    `;
    rows.appendChild(tr);
  });
}

async function refresh() {
  try {
    const res = await fetch("/api/top");
    const data = await res.json();
    allMovers = data.movers || [];
    allMovers.forEach(m => knownSports.add(m.sport));
    updateSportOptions();
    const meta = document.getElementById("meta");
    meta.textContent = `${data.markets_tracked} markets tracked (${(data.sports_filter || []).join(", ")}) - lookback ${data.lookback_minutes} min - `
      + `alert threshold ${(data.price_move_threshold * 100).toFixed(1)} pts - updated ${data.generated_at || "-"}`;
    render();
  } catch (e) {
    document.getElementById("meta").textContent = "Failed to reach bot - is it still running?";
  }
}

document.getElementById("search").addEventListener("input", render);
document.getElementById("sportFilter").addEventListener("change", render);
refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # keep the bot's log clean; the poll loop already logs cycles

    def do_GET(self):
        if self.path.startswith("/api/top"):
            with dashboard_lock:
                payload = {
                    "generated_at": dashboard_state["generated_at"],
                    "markets_tracked": dashboard_state["markets_tracked"],
                    "sports_filter": dashboard_state["sports_filter"],
                    "lookback_minutes": LOOKBACK_MINUTES,
                    "price_move_threshold": PRICE_MOVE_THRESHOLD,
                    "movers": dashboard_state["movers"],
                }
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            body = DASHBOARD_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)


def start_dashboard_server():
    server = ThreadingHTTPServer((DASHBOARD_HOST, DASHBOARD_PORT), DashboardHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log.info("Dashboard running at http://%s:%d", DASHBOARD_HOST, DASHBOARD_PORT)


def main():
    log.info(
        "Starting bot: poll=%ss lookback=%smin price_threshold=%.3f cooldown=%smin",
        POLL_INTERVAL_SECONDS, LOOKBACK_MINUTES, PRICE_MOVE_THRESHOLD, COOLDOWN_MINUTES,
    )
    if not NTFY_TOPIC:
        log.warning("ntfy is not configured yet - see .env.example")
    start_dashboard_server()
    while True:
        try:
            run_once()
        except Exception as e:
            log.exception("Cycle failed: %s", e)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
