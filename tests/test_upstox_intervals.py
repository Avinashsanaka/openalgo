from broker.upstox.api.data import BrokerData
import unittest

class TestUpstoxIntervals(unittest.TestCase):
    def test_interval_mapping(self):
        bd = BrokerData("dummy_token")

        # Test existing
        self.assertIn('D', bd.timeframe_map)
        self.assertEqual(bd.timeframe_map['D'], {'unit': 'days', 'interval': '1'})

        # Test new aliases
        self.assertIn('1d', bd.timeframe_map)
        self.assertEqual(bd.timeframe_map['1d'], {'unit': 'days', 'interval': '1'})

        self.assertIn('1wk', bd.timeframe_map)
        self.assertEqual(bd.timeframe_map['1wk'], {'unit': 'weeks', 'interval': '1'})

        self.assertIn('1mo', bd.timeframe_map)
        self.assertEqual(bd.timeframe_map['1mo'], {'unit': 'months', 'interval': '1'})

if __name__ == '__main__':
    unittest.main()
