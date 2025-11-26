import unittest
import os
import sqlite3
from sqlalchemy import create_engine, text
from unittest.mock import patch, MagicMock

# Import the module to test
# We need to do this carefully if we want to patch the engine BEFORE it's used?
# Actually, we can patch the engine object that the module has already created,
# or patch where it's used.
# init_db uses the global 'engine' variable.

class TestManagementDBMigration(unittest.TestCase):
    def setUp(self):
        self.test_db = 'test_management.db'
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

            # Run migration
            # We mock init_db_with_logging to avoid creating tables via metadata
            # (which would try to create the full table and might conflict or do nothing if table exists)
            # However, init_db_with_logging usually does Base.metadata.create_all(bind=engine)
            # Since table exists, create_all does nothing.
            # Then the migration logic kicks in.

            with patch('database.db_init_helper.init_db_with_logging') as mock_init:
                init_db()

            # Verify columns were added
            with test_engine.connect() as conn:
                result = conn.execute(text("PRAGMA table_info(management_rules)"))
                columns = [row[1] for row in result]

                self.assertIn('target_profit', columns, "target_profit column should have been added")
                self.assertIn('is_group_rule', columns, "is_group_rule column should have been added")

if __name__ == '__main__':
    unittest.main()
