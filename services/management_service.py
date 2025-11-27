import threading
import time
import json
import zmq
import pandas as pd
from datetime import datetime, time as dtime
import pytz
from database.management_db import ManagementRule, db_session, delete_rule
from services.positionbook_service import get_positionbook
from services.place_order_service import place_order
from database.auth_db import get_auth_token_broker, get_api_key_for_tradingview as get_api_key_for_user
from utils.logging import get_logger
import os

logger = get_logger(__name__)

# Cache for OHLCV data: {symbol: {ltp, open, high, low, close, volume}}
ohlcv_cache = {}

def is_market_open():
    """Check if current time is within Indian Market hours (09:15 - 15:30 IST)"""
    # Note: This hardcodes IST market hours. Future versions should support configurable hours/timezones.
    try:
        tz = pytz.timezone('Asia/Kolkata')
        now = datetime.now(tz)

        # Weekends (5=Sat, 6=Sun)
        if now.weekday() > 4:
            return False

        current_time = now.time()
        market_start = dtime(9, 15)
        market_end = dtime(15, 30)

        return market_start <= current_time <= market_end
    except Exception as e:
        logger.error(f"Error checking market hours: {e}")
        return True # Fail open to avoid blocking if timezone fails

def get_net_qty(p):
    """Helper to safely get net quantity from position dictionary"""
    for key in ['quantity', 'netqty', 'net_qty', 'qty']:
        if key in p:
            try:
                return float(p[key])
            except (ValueError, TypeError):
                continue
    return 0

def calculate_pnl(pos):
    """Calculate PnL for a position using cached LTP if available"""
    # Default to API PnL
    try:
        pnl = float(pos.get('pnl', 0))
    except (ValueError, TypeError):
        pnl = 0.0

    # Try to calculate more recent PnL using ZMQ cache
    symbol = pos.get('symbol')
    if symbol in ohlcv_cache and 'ltp' in ohlcv_cache[symbol]:
        ltp = ohlcv_cache[symbol]['ltp']
        net_qty = get_net_qty(pos)

        if 'netavgprc' in pos:
            try:
                avg_price = float(pos['netavgprc'])
                # Basic PnL calculation
                if net_qty > 0: # Long
                    pnl = (ltp - avg_price) * net_qty
                elif net_qty < 0: # Short
                    pnl = (avg_price - ltp) * abs(net_qty)
            except (ValueError, TypeError):
                pass
    return pnl

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

    # State tracking for logging
    market_was_open = False
    last_log_time = time.time()
    tick_count = 0

    while True:
        try:
            current_market_status = is_market_open()

            # Log transitions
            if current_market_status and not market_was_open:
                logger.info("Market is now OPEN (IST). Resuming data processing and rule checks.")
            elif not current_market_status and market_was_open:
                logger.info("Market is now CLOSED (IST). Pausing processing.")

            market_was_open = current_market_status

            # 1. Check ZMQ for market data updates (non-blocking or short timeout)
            socks = dict(poller.poll(100)) # 100ms timeout
            if socket in socks and socks[socket] == zmq.POLLIN:
                topic, message = socket.recv_multipart()

                # Update cache only during market hours (as per requirement)
                if current_market_status:
                    process_market_data(topic, message)
                    tick_count += 1

            # Periodic Heartbeat Log (every 60s) during market hours
            if current_market_status and time.time() - last_log_time > 60:
                if tick_count > 0:
                    logger.info(f"Management Service Active: Processed {tick_count} market data ticks in last minute.")
                else:
                    logger.info("Management Service Active: No market data received in last minute (waiting for ticks).")
                tick_count = 0
                last_log_time = time.time()

            # 2. Every few seconds, check rules against current state
            # Only check rules if market is open
            if current_market_status:
                check_rules()
            else:
                # Sleep a bit longer if market is closed to save CPU
                time.sleep(5)

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
        # Simplified: grab symbol from parts or data
        symbol = data.get('symbol') or (parts[-2] if len(parts) >= 3 else None)

        if not symbol:
            return

        # Update cache with available fields
        if symbol not in ohlcv_cache:
            ohlcv_cache[symbol] = {}

        # Standardize and store fields
        if 'ltp' in data: ohlcv_cache[symbol]['ltp'] = float(data['ltp'])
        if 'open' in data: ohlcv_cache[symbol]['open'] = float(data['open'])
        if 'high' in data: ohlcv_cache[symbol]['high'] = float(data['high'])
        if 'low' in data: ohlcv_cache[symbol]['low'] = float(data['low'])
        if 'close' in data: ohlcv_cache[symbol]['close'] = float(data['close'])
        if 'volume' in data: ohlcv_cache[symbol]['volume'] = float(data['volume'])

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
                        # --- Group Rule Logic ---
                        if rule.is_group_rule:
                            matching_positions = []
                            group_pnl = 0.0

                            # Iterate all positions to find matches (prefix match on symbol)
                            for p_key, p_data in positions_map.items():
                                # Check matching criteria: Symbol prefix AND Product
                                if p_data['symbol'].startswith(rule.symbol) and p_data['product'] == rule.product:
                                    qty = get_net_qty(p_data)
                                    if qty != 0:
                                        matching_positions.append(p_data)
                                        group_pnl += calculate_pnl(p_data)

                            if not matching_positions:
                                continue

                            # Check Target Profit
                            if rule.target_profit and group_pnl >= rule.target_profit:
                                logger.info(f"Group Target Profit Triggered for {rule.symbol}: PnL {group_pnl} >= {rule.target_profit}")
                                for pos in matching_positions:
                                    execute_exit(rule, pos, api_key, f"Group Target Profit: {group_pnl}")

                            # Check Max Loss (Combined)
                            elif rule.max_loss and group_pnl <= -abs(rule.max_loss):
                                logger.info(f"Group Max Loss Triggered for {rule.symbol}: PnL {group_pnl} <= -{rule.max_loss}")
                                for pos in matching_positions:
                                    execute_exit(rule, pos, api_key, f"Group Max Loss: {group_pnl}")

                        # --- Individual Rule Logic ---
                        else:
                            pos_key = f"{rule.symbol}_{rule.product}"
                            pos = positions_map.get(pos_key)

                            if not pos:
                                continue

                            net_qty = get_net_qty(pos)
                            if net_qty == 0:
                                continue

                            # Calculate PnL
                            pnl = calculate_pnl(pos)

                            # Check Target Profit (Individual)
                            if rule.target_profit and pnl >= rule.target_profit:
                                logger.info(f"Target Profit Triggered for {rule.symbol}: PnL {pnl} >= {rule.target_profit}")
                                execute_exit(rule, pos, api_key, "Target Profit Triggered")

                            # Check Total Loss (Individual)
                            elif rule.exit_type in ['TOTAL_LOSS', 'BOTH'] and rule.max_loss:
                                # If PnL is negative and absolute value > max_loss
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
        net_qty = get_net_qty(position)
        qty = abs(int(net_qty))
        action = 'SELL' if net_qty > 0 else 'BUY'

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
