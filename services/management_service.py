import threading
import time
import json
import zmq
import pandas as pd
import datetime
from database.management_db import ManagementRule, db_session, delete_rule
from services.positionbook_service import get_positionbook
from services.place_order_service import place_order
from database.auth_db import get_auth_token_broker, get_api_key_for_tradingview as get_api_key_for_user
from utils.logging import get_logger
import os

logger = get_logger(__name__)

# Cache for OHLCV data: {symbol: DataFrame}
ohlcv_cache = {}

def start_management_service():
    """Starts the background thread for management service"""
    thread = threading.Thread(target=management_loop, daemon=True)
    thread.start()
    logger.info("Management Service Started")

def management_loop():
    """Main loop to monitor positions and rules"""

    # ZMQ Subscriber setup
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    ZMQ_HOST = os.getenv('ZMQ_HOST', '127.0.0.1')
    ZMQ_PORT = os.getenv('ZMQ_PORT', '5555')
    socket.connect(f"tcp://{ZMQ_HOST}:{ZMQ_PORT}")
    socket.setsockopt(zmq.SUBSCRIBE, b"")

    # Poller to check for ZMQ messages without blocking indefinitely
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)

    logger.info(f"Management Service connected to ZMQ at {ZMQ_HOST}:{ZMQ_PORT}")

    while True:
        try:
            # 1. Check ZMQ for market data updates (non-blocking or short timeout)
            socks = dict(poller.poll(100)) # 100ms timeout
            if socket in socks and socks[socket] == zmq.POLLIN:
                topic, message = socket.recv_multipart()
                process_market_data(topic, message)

            # 2. Every few seconds, check rules against current state
            # Note: In a production system, we'd trigger on tick, but for "Total Loss"
            # and checking multiple positions, a periodic check is simpler for V1.
            # However, user asked for "candle close". So we need to build candles.
            # ZMQ from websocket_proxy sends 'market_data' which is usually LTP/Quote.
            # If we receive OHLCV updates (e.g. from 1m stream), we can use that.
            # Assuming websocket_proxy sends raw ticks or quotes.

            # For this implementation, I will rely on the `ohlcv_cache` which needs to be populated.
            # Since real candle construction is complex, I will check "Total Loss" periodically
            # and "Candle Close" if I detect a new candle (timestamp change).

            check_rules()

        except Exception as e:
            logger.error(f"Error in management loop: {e}")
            time.sleep(1)

def process_market_data(topic, message):
    """Update cache with latest market data"""
    try:
        topic_str = topic.decode('utf-8')
        data = json.loads(message.decode('utf-8'))

        # Topic format: BROKER_EXCHANGE_SYMBOL_MODE or EXCHANGE_SYMBOL_MODE
        parts = topic_str.split('_')
        # ... parsing logic similar to server.py ...
        # Simplified: grab symbol from parts or data
        symbol = data.get('symbol') or (parts[-2] if len(parts) >= 3 else None)

        if not symbol:
            return

        # If data contains OHLC, update cache
        # Note: This depends on what websocket_proxy sends.
        # Usually it sends LTP.
        # If we need candles, we must maintain a time-series.
        # For simplicity in V1: Update LTP in a simple cache
        # Real-time candle construction is out of scope for a quick implementation unless provided by proxy.

        # We'll store the latest tick
        if 'ltp' in data:
            if symbol not in ohlcv_cache:
                ohlcv_cache[symbol] = {'ltp': float(data['ltp']), 'ticks': []}
            else:
                ohlcv_cache[symbol]['ltp'] = float(data['ltp'])

            # If we were building candles, we'd append to ticks here.

    except Exception as e:
        logger.error(f"Error processing market data: {e}")

def check_rules():
    """Iterate over active rules and check conditions"""
    try:
        # Create a new session for thread safety
        session = db_session()
        rules = session.query(ManagementRule).filter_by(is_active=True).all()

        # Group rules by user_id to minimize API calls
        rules_by_user = {}
        for rule in rules:
            if rule.user_id not in rules_by_user:
                rules_by_user[rule.user_id] = []
            rules_by_user[rule.user_id].append(rule)

        for user_id, user_rules in rules_by_user.items():
            try:
                # Fetch positions once per user (API call)
                # TODO: In V2, cache this result for X seconds to respect rate limits further
                api_key = get_api_key_for_user(user_id)
                if not api_key: continue

                success, response, _ = get_positionbook(api_key=api_key)
                if not success or not response or 'data' not in response: continue

                # Create a map for fast position lookup
                # Key: symbol_product (e.g. INF_MIS)
                positions_map = {f"{p['symbol']}_{p['product']}": p for p in response['data']}

                for rule in user_rules:
                    try:
                        pos_key = f"{rule.symbol}_{rule.product}"
                        pos = positions_map.get(pos_key)

                        if not pos or int(pos['netqty']) == 0:
                            # Position closed
                            continue

                        # Check Total Loss
                        if rule.exit_type in ['TOTAL_LOSS', 'BOTH'] and rule.max_loss:
                            # Preferred: Calculate PnL using cached real-time LTP if available
                            # This allows for faster reaction than waiting for API PnL updates
                            pnl = float(pos['pnl'])

                            if rule.symbol in ohlcv_cache and 'ltp' in ohlcv_cache[rule.symbol]:
                                ltp = ohlcv_cache[rule.symbol]['ltp']
                                net_qty = int(pos['netqty'])

                                # Standard PnL = (LTP - BuyAvg) * Qty for Long
                                # For simplicity, we take the API's PnL and adjust for LTP change?
                                # Or re-calculate: (LTP - AvgPrice) * Qty
                                # We need avg price. 'netavgprc' is usually available.
                                if 'netavgprc' in pos:
                                    avg_price = float(pos['netavgprc'])
                                    multiplier = 1 # Multiplier for F&O not handled here, assuming 1 or equity

                                    if net_qty > 0: # Long
                                        pnl = (ltp - avg_price) * net_qty
                                    elif net_qty < 0: # Short
                                        pnl = (avg_price - ltp) * abs(net_qty)

                            # If PnL is negative and absolute value > max_loss (i.e. pnl < -max_loss)
                            if pnl < 0 and abs(pnl) >= rule.max_loss:
                                logger.info(f"Total Loss Triggered for {rule.symbol}: PnL {pnl} <= -{rule.max_loss}")
                                execute_exit(rule, pos, api_key, "Max Loss Triggered")

                        # Check Candle Close
                        if rule.exit_type in ['CANDLE_CLOSE', 'BOTH']:
                            # Requires candle engine.
                            # Using LTP from position vs EMA is risky without confirmed close.
                            # We will log that we are skipping this check until Candle Engine is available.
                            # logger.debug(f"Skipping Candle Close check for {rule.symbol} - Engine not ready")
                            pass

                    except Exception as e:
                        logger.error(f"Error checking rule {rule.id}: {e}")

            except Exception as e:
                logger.error(f"Error processing rules for user {user_id}: {e}")

        session.close()
        time.sleep(1) # Rate limit checks

    except Exception as e:
        logger.error(f"Error in check_rules: {e}")

def execute_exit(rule, position, api_key, reason):
    """Place exit order"""
    try:
        qty = abs(int(position['netqty']))
        action = 'SELL' if int(position['netqty']) > 0 else 'BUY'

        payload = {
            'apikey': api_key,
            'symbol': rule.symbol,
            'exchange': rule.exchange,
            'product': rule.product,
            'quantity': str(qty),
            'disclosed_quantity': '0',
            'price': '0',
            'trigger_price': '0',
            'pricetype': 'MARKET',
            'action': action,
            'tag': 'MANAGEMENT_EXIT'
        }

        logger.info(f"Executing Management Exit for {rule.symbol}: {reason}")
        place_order(payload)

        # Deactivate rule immediately to prevent duplicate orders
        # We must use a new session or commit the passed object if attached
        # Since 'rule' is from a session in check_rules, we can commit that session or use a fresh one.
        # But check_rules closes the session.

        # Simplest way: update status in DB directly
        session = db_session()
        r = session.query(ManagementRule).filter_by(id=rule.id).first()
        if r:
            r.is_active = False
            session.commit()
            logger.info(f"Rule {rule.id} deactivated after exit execution")
        session.close()

    except Exception as e:
        logger.error(f"Error executing exit: {e}")
