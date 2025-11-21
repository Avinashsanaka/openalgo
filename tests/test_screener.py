import unittest
from unittest.mock import MagicMock, patch
from flask import Flask, session
from blueprints.screener import screener_bp

class TestScreener(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.register_blueprint(screener_bp)
        self.app.secret_key = 'test'
        self.client = self.app.test_client()

    @patch('blueprints.screener.enhanced_search_symbols')
    @patch('blueprints.screener.get_auth_token')
    @patch('blueprints.screener.get_feed_token')
    @patch('blueprints.screener.get_history')
    def test_scan_vbl_success(self, mock_get_history, mock_get_feed, mock_get_auth, mock_search):
        # Mock tokens
        mock_get_auth.return_value = 'auth_token'
        mock_get_feed.return_value = 'feed_token'

        # Mock symbol search
        mock_symbol = MagicMock()
        mock_symbol.symbol = 'VBL-EQ'
        mock_symbol.instrumenttype = 'EQ'
        mock_search.return_value = [mock_symbol]

        # Mock History Data (enough for 20 EMA)
        mock_data = [{'date': f'2023-01-{i:02d}', 'close': 100 + i} for i in range(1, 30)]
        mock_get_history.return_value = (True, {'status': 'success', 'data': mock_data}, 200)

        with self.client.session_transaction() as sess:
            sess['user'] = 'testuser'
            sess['broker'] = 'angel'

        response = self.client.get('/screener/scan_vbl')
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['data']['symbol'], 'VBL-EQ')
        self.assertIn('signal', data['data'])

        # Price is increasing (100 to 129), so last price > EMA
        self.assertEqual(data['data']['signal'], 'ABOVE 20 EMA')

    def test_scan_vbl_no_broker(self):
         with self.client.session_transaction() as sess:
            sess['user'] = 'testuser'
            # No broker in session

         response = self.client.get('/screener/scan_vbl')
         self.assertEqual(response.status_code, 400)

if __name__ == '__main__':
    unittest.main()
