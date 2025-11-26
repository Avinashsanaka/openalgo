import os
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, Text
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import NullPool
from datetime import datetime
from utils.logging import get_logger

logger = get_logger(__name__)

# Use the same DATABASE_URL as other DBs (or a specific one if needed)
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///database.db')

if 'sqlite' in DATABASE_URL:
    engine = create_engine(DATABASE_URL, poolclass=NullPool, connect_args={'check_same_thread': False})
else:
    engine = create_engine(DATABASE_URL, pool_size=50, max_overflow=100, pool_timeout=10)

db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()

class ManagementRule(Base):
    __tablename__ = 'management_rules'

    id = Column(Integer, primary_key=True)
    user_id = Column(String, index=True, nullable=False)
    symbol = Column(String, nullable=False)
    exchange = Column(String, nullable=False)
    product = Column(String, nullable=False)

    # Exit Conditions
    exit_type = Column(String, default='NONE') # 'CANDLE_CLOSE', 'TOTAL_LOSS', 'BOTH'

    # Candle Close params (e.g., {"indicator": "EMA", "period": 20, "condition": "BELOW"})
    candle_condition = Column(Text, nullable=True)

    # Total Loss params (e.g., absolute amount)
    max_loss = Column(Float, nullable=True)

    # Target Profit params (e.g., absolute amount)
    target_profit = Column(Float, nullable=True)

    # Group Rule (Apply to all matching symbols)
    is_group_rule = Column(Boolean, default=False)

    # Status
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)
    last_triggered = Column(DateTime, nullable=True)

def init_db():
    from database.db_init_helper import init_db_with_logging
    init_db_with_logging(Base, engine, "Management DB", logger)

    # Auto-migration logic for SQLite
    if 'sqlite' in str(engine.url):
        try:
            from sqlalchemy import text
            with engine.connect() as conn:
                # Get existing columns
                result = conn.execute(text("PRAGMA table_info(management_rules)"))
                columns = [row[1] for row in result]

                # Add target_profit if missing
                if 'target_profit' not in columns:
                    logger.info("Migrating Management DB: Adding target_profit column")
                    try:
                        conn.execute(text("ALTER TABLE management_rules ADD COLUMN target_profit FLOAT"))
                        conn.commit()
                    except Exception as e:
                        logger.warning(f"Failed to add target_profit column: {e}")

                # Add is_group_rule if missing
                if 'is_group_rule' not in columns:
                    logger.info("Migrating Management DB: Adding is_group_rule column")
                    try:
                        conn.execute(text("ALTER TABLE management_rules ADD COLUMN is_group_rule BOOLEAN DEFAULT 0"))
                        conn.commit()
                    except Exception as e:
                        logger.warning(f"Failed to add is_group_rule column: {e}")

        except Exception as e:
            logger.error(f"Error during Management DB schema verification: {e}")

def get_rules_for_user(user_id):
    return ManagementRule.query.filter_by(user_id=user_id).all()

def add_rule(user_id, symbol, exchange, product, exit_type, candle_condition=None, max_loss=None, target_profit=None, is_group_rule=False):
    rule = ManagementRule(
        user_id=user_id,
        symbol=symbol,
        exchange=exchange,
        product=product,
        exit_type=exit_type,
        candle_condition=candle_condition,
        max_loss=max_loss,
        target_profit=target_profit,
        is_group_rule=is_group_rule
    )
    db_session.add(rule)
    db_session.commit()
    return rule

def delete_rule(rule_id, user_id):
    rule = ManagementRule.query.filter_by(id=rule_id, user_id=user_id).first()
    if rule:
        db_session.delete(rule)
        db_session.commit()
        return True
    return False
