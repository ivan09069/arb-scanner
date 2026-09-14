#!/usr/bin/env python3
"""
Arbitrage Scanner - Find price discrepancies across DEXes
Monitors Uniswap, Aerodrome, BaseSwap, SushiSwap
"""
import os
import asyncio
import aiohttp
import json
import logging
import functools
import hashlib
import hmac
from datetime import datetime, timezone
from flask import Flask, request, jsonify
import threading

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger("ArbScanner")

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 16384

API_KEY = os.environ.get("API_KEY", "")
TRADE_EXECUTOR_URL = os.environ.get("TRADE_EXECUTOR_URL", "https://trade-executor-service.onrender.com")
TRADE_EXECUTOR_KEY = os.environ.get("TRADE_EXECUTOR_KEY", "")
MIN_PROFIT_PERCENT = float(os.environ.get("MIN_PROFIT_PERCENT", "0.5"))

def require_auth(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get('Authorization', '')
        key = request.headers.get('X-API-Key', '')
        if len(API_KEY) < 32:
            return jsonify({"error": "API authentication not configured"}), 503
        def matches(value):
            return hmac.compare_digest(hashlib.sha256(value.encode()).digest(), hashlib.sha256(API_KEY.encode()).digest())
        bearer = auth[7:] if auth.startswith('Bearer ') else ''
        if matches(bearer) or matches(key):
            return f(*args, **kwargs)
        return jsonify({"error": "Unauthorized"}), 401
    return decorated

# DexScreener API for prices
DEXSCREENER_API = "https://api.dexscreener.com/latest/dex/tokens/"

# Tokens to monitor for arbitrage
TOKENS = {
    "base": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "AERO": "0x940181a94A35A4569E4529A3CDfB74e38FD98631",
    }
}

opportunities = []
scanning = False

async def get_token_prices(session, token_address):
    try:
        async with session.get(f"{DEXSCREENER_API}{token_address}", timeout=15) as r:
            data = await r.json()
            pairs = data.get("pairs", [])
            prices = {}
            for pair in pairs[:10]:
                dex = pair.get("dexId", "unknown")
                price = float(pair.get("priceUsd", 0))
                if price > 0:
                    prices[dex] = {"price": price, "liquidity": pair.get("liquidity", {}).get("usd", 0)}
            return prices
    except Exception as e:
        log.error(f"Price fetch error: {e}")
    return {}

async def find_arbitrage(session, chain, token_name, token_address):
    prices = await get_token_prices(session, token_address)
    if len(prices) < 2:
        return None
    
    sorted_prices = sorted(prices.items(), key=lambda x: x[1]["price"])
    low_dex, low_data = sorted_prices[0]
    high_dex, high_data = sorted_prices[-1]
    
    spread = ((high_data["price"] - low_data["price"]) / low_data["price"]) * 100
    
    if spread >= MIN_PROFIT_PERCENT:
        return {
            "token": token_name,
            "chain": chain,
            "buy_dex": low_dex,
            "buy_price": low_data["price"],
            "sell_dex": high_dex,
            "sell_price": high_data["price"],
            "spread_percent": round(spread, 3),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    return None

async def send_signal(opp):
    if not TRADE_EXECUTOR_KEY:
        return
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{TRADE_EXECUTOR_URL}/signal",
                json={
                    "source": "arb-scanner",
                    "action": "swap",
                    "chain": opp["chain"],
                    "token_in": "USDC",
                    "token_out": opp["token"],
                    "amount": 100,
                    "confidence": min(opp["spread_percent"] / 2, 0.9),
                    "reason": f"Arb opportunity: {opp['spread_percent']:.2f}% spread"
                },
                headers={"X-API-Key": TRADE_EXECUTOR_KEY},
                timeout=10
            ) as r:
                log.info(f"Signal sent: {r.status}")
    except Exception as e:
        log.error(f"Signal failed: {e}")

async def scan_loop():
    global scanning
    log.info("Arbitrage scanning started")
    
    while scanning:
        async with aiohttp.ClientSession() as session:
            for chain, tokens in TOKENS.items():
                for token_name, token_address in tokens.items():
                    opp = await find_arbitrage(session, chain, token_name, token_address)
                    if opp:
                        opportunities.append(opp)
                        log.info(f"💰 ARB: {opp['token']} {opp['spread_percent']:.2f}% ({opp['buy_dex']} → {opp['sell_dex']})")
                        await send_signal(opp)
        
        await asyncio.sleep(30)

def start_scanner():
    global scanning
    scanning = True
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(scan_loop())

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "service": "arb-scanner", "scanning": scanning, "opportunities": len(opportunities), "timestamp": datetime.now(timezone.utc).isoformat()})

@app.route('/opportunities', methods=['GET'])
@require_auth
def get_opportunities():
    return jsonify({"opportunities": opportunities[-50:]})

@app.route('/tokens', methods=['GET', 'POST'])
@require_auth
def manage_tokens():
    if request.method == 'POST':
        data = request.json
        chain = data.get("chain", "base")
        if chain not in TOKENS:
            TOKENS[chain] = {}
        TOKENS[chain][data["symbol"]] = data["address"]
        return jsonify({"status": "added"})
    return jsonify({"tokens": TOKENS})

@app.route('/start', methods=['POST'])
@require_auth
def start():
    global scanning
    if not scanning:
        thread = threading.Thread(target=start_scanner, daemon=True)
        thread.start()
        return jsonify({"status": "started"})
    return jsonify({"status": "already running"})

@app.route('/stop', methods=['POST'])
@require_auth
def stop():
    global scanning
    scanning = False
    return jsonify({"status": "stopped"})

if __name__ == "__main__":
    thread = threading.Thread(target=start_scanner, daemon=True)
    thread.start()
    port = int(os.environ.get("PORT", 10000))
    log.info(f"Arbitrage Scanner (AUTH ENABLED) starting on port {port}")
    app.run(host="0.0.0.0", port=port)
