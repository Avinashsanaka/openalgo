import unittest
import os
import sqlite3
from sqlalchemy import create_engine, text
from unittest.mock import patch, MagicMock

class TestManagementDBMigration(unittest.TestCase):
    def setUp(self):
        self.test_db = 'test_management_v2.db'
        if os.path.exists(self.test_db):
            os.remove(self.test_db)

        # Create a "legacy" database
        self.conn = sqlite3.connect(self.test_db)
        self.cursor = self.conn.cursor()

        # Create table WITHOUT the new columns (target_profit, is_group_rule)
        self.cursor.execute('''
            CREATE TABLE management_rules (
                id INTEGER PRIMARY KEY,
                user_id VARCHAR NOT NULL,
                symbol VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL,
                product VARCHAR NOT NULL,
                exit_type VARCHAR,
                candle_condition TEXT,
                max_loss FLOAT,
                is_active BOOLEAN,
                created_at DATETIME,
                last_triggered DATETIME
            )
        ''')
        self.conn.commit()
        self.conn.close()

    def tearDown(self):
        if os.path.exists(self.test_db):
            os.remove(self.test_db)

    def test_migration_adds_columns(self):
        # Create an engine pointing to our test DB
        test_engine = create_engine(f'sqlite:///{self.test_db}')

        # Patch the engine in the module
        with patch('database.management_db.engine', test_engine):
            from database.management_db import init_db

            # Mock init_db_with_logging to prevent it from trying to create tables via metadata
            # which might interfere with our pre-created table test
            with patch('database.db_init_helper.init_db_with_logging'):
                init_db()

            # Verify columns were added
            with test_engine.connect() as conn:
                result = conn.execute(text("PRAGMA table_info(management_rules)"))
                columns = [row[1] for row in result]

                self.assertIn('target_profit', columns, "target_profit column should have been added")
                self.assertIn('is_group_rule', columns, "is_group_rule column should have been added")
                self.assertIn('included_positions', columns, "included_positions column should have been added")

if __name__ == '__main__':
    unittest.main()
