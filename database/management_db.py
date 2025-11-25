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

    # Status
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)
    last_triggered = Column(DateTime, nullable=True)

def init_db():
    from database.db_init_helper import init_db_with_logging
    init_db_with_logging(Base, engine, "Management DB", logger)

def get_rules_for_user(user_id):
    return ManagementRule.query.filter_by(user_id=user_id).all()

def add_rule(user_id, symbol, exchange, product, exit_type, candle_condition=None, max_loss=None):
    rule = ManagementRule(
        user_id=user_id,
        symbol=symbol,
        exchange=exchange,
        product=product,
        exit_type=exit_type,
        candle_condition=candle_condition,
        max_loss=max_loss
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
