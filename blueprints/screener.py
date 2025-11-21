from flask import Blueprint, render_template, session, request, jsonify
from services.history_service import get_history
from database.auth_db import get_auth_token, get_feed_token
import pandas as pd
import datetime
from utils.logging import get_logger

logger = get_logger(__name__)

screener_bp = Blueprint('screener_bp', __name__, url_prefix='/screener')

@screener_bp.route('/')
def index():
    return render_template('screener.html')

@screener_bp.route('/scan_vbl')
def scan_vbl():
    """
    Scans VBL for 20 EMA condition.
    """
    try:
        # Get user credentials from session
        if 'user' not in session:
            return jsonify({'status': 'error', 'message': 'User not logged in'}), 401

        username = session['user']
        broker = session.get('broker')

        if not broker:
             return jsonify({'status': 'error', 'message': 'Broker not selected'}), 400

        # Get tokens
        auth_token = get_auth_token(username)
        feed_token = get_feed_token(username)

        if not auth_token:
             return jsonify({'status': 'error', 'message': 'Auth token not found'}), 401

        # Parameters for VBL
        symbol = "VBL-EQ" # Assuming standard NSE symbol format
        exchange = "NSE"
        interval = "1d" # Daily candles

        # Calculate dates
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=60) # Get enough data for 20 EMA

        str_end_date = end_date.strftime('%Y-%m-%d')
        str_start_date = start_date.strftime('%Y-%m-%d')

        # Fetch History
        success, response, code = get_history(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start_date=str_start_date,
            end_date=str_end_date,
            auth_token=auth_token,
            feed_token=feed_token,
            broker=broker
        )

        if not success or 'data' not in response:
             return jsonify({'status': 'error', 'message': f"Failed to fetch data: {response.get('message')}"}), code

        data = response['data']
        if not data:
            return jsonify({'status': 'error', 'message': 'No data returned'}), 404

        # Create DataFrame
        df = pd.DataFrame(data)

        # Check if 'close' column exists
        if 'close' not in df.columns:
             # Try 'Close' or 'c' depending on broker data format, but typically it should be 'close' if normalized.
             # Let's inspect what we have if it fails, but standardizing on lowercase 'close' is best if service does it.
             # The history_service returns what broker returns.
             pass

        # Ensure numeric
        df['close'] = pd.to_numeric(df['close'])

        # Calculate 20 EMA
        df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()

        last_row = df.iloc[-1]
        current_price = last_row['close']
        ema_20 = last_row['ema_20']

        # Determine signal
        signal = "NEUTRAL"
        if current_price > ema_20:
            signal = "ABOVE 20 EMA"
        else:
            signal = "BELOW 20 EMA"

        result = {
            'symbol': symbol,
            'current_price': float(current_price),
            'ema_20': float(ema_20),
            'signal': signal,
            'timestamp': str(last_row.get('date', ''))
        }

        return jsonify({'status': 'success', 'data': result})

    except Exception as e:
        logger.error(f"Error in scan_vbl: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500
