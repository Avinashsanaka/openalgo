from flask import Blueprint, render_template, session, request, jsonify, flash, redirect, url_for
from database.auth_db import get_auth_token
from services.positionbook_service import get_positionbook_with_auth
from database.management_db import get_rules_for_user, add_rule, delete_rule
import json
from utils.logging import get_logger

logger = get_logger(__name__)

management_bp = Blueprint('management_bp', __name__, url_prefix='/strategy/management')

@management_bp.route('/')
def index():
    if 'user' not in session:
        return redirect(url_for('auth.login'))

    username = session['user']
    broker = session.get('broker')

    # Fetch Open Positions
    positions = []
    if broker:
        auth_token = get_auth_token(username)
        if auth_token:
            success, response, _ = get_positionbook_with_auth(auth_token, broker)
            if success:
                # Filter for open positions (netqty != 0)
                # Normalize netqty to ensure it exists for template
                def get_net_qty(p):
                    for key in ['quantity', 'netqty', 'net_qty', 'qty']:
                        if key in p:
                            try:
                                return float(p[key])
                            except (ValueError, TypeError):
                                continue
                    return 0

                filtered_positions = []
                for p in response.get('data', []):
                    qty = get_net_qty(p)
                    if qty != 0:
                        p['netqty'] = qty  # Ensure template has access to netqty
                        filtered_positions.append(p)
                positions = filtered_positions
            else:
                logger.error(f"Failed to fetch positions for {username}: {response}")
        else:
            logger.warning(f"No auth token found for {username}")
    else:
        logger.warning(f"No broker in session for {username}")

    # Fetch Rules
    rules = get_rules_for_user(username)
    rules_map = {(r.symbol, r.product): r for r in rules}

    # Merge rules with positions
    merged_positions = []
    for p in positions:
        rule = rules_map.get((p['symbol'], p['product']))
        p['rule'] = rule
        merged_positions.append(p)

    return render_template('management/index.html', positions=merged_positions)

@management_bp.route('/add_rule', methods=['POST'])
def add_rule_route():
    if 'user' not in session:
        return jsonify({'status': 'error', 'message': 'Not logged in'}), 401

    data = request.json
    username = session['user']

    try:
        symbol = data['symbol']
        exchange = data['exchange']
        product = data['product']
        exit_type = data['exit_type'] # 'CANDLE_CLOSE', 'TOTAL_LOSS', 'BOTH'

        candle_condition = json.dumps(data.get('candle_condition')) if data.get('candle_condition') else None
        max_loss = float(data.get('max_loss')) if data.get('max_loss') else None
        target_profit = float(data.get('target_profit')) if data.get('target_profit') else None
        is_group_rule = data.get('is_group_rule', False)

        add_rule(username, symbol, exchange, product, exit_type, candle_condition, max_loss, target_profit, is_group_rule)
        return jsonify({'status': 'success'})
    except Exception as e:
        logger.error(f"Error adding rule: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@management_bp.route('/delete_rule', methods=['POST'])
def delete_rule_route():
    if 'user' not in session:
        return jsonify({'status': 'error', 'message': 'Not logged in'}), 401

    data = request.json
    rule_id = data['rule_id']
    username = session['user']

    if delete_rule(rule_id, username):
        return jsonify({'status': 'success'})
    else:
        return jsonify({'status': 'error', 'message': 'Rule not found'}), 404
