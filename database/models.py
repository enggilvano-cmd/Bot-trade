from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, BigInteger, Boolean
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()

class Kline(Base):
    __tablename__ = 'klines'
    id = Column(Integer, primary_key=True)
    symbol = Column(String, index=True)
    timestamp = Column(DateTime, unique=True, index=True) # Torna o timestamp único para evitar duplicatas
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)

    def __repr__(self):
        return f"<Kline(symbol='{self.symbol}', timestamp='{self.timestamp}', close={self.close})>"

class Order(Base):
    __tablename__ = 'orders'
    id = Column(Integer, primary_key=True, autoincrement=True)
    client_order_id = Column(String, unique=True, index=True) # ID gerado por nós
    order_id = Column(String, unique=True, index=True, nullable=True) # ID retornado pela Bybit
    symbol = Column(String, index=True)
    side = Column(String)
    order_type = Column(String, default="Market")
    price = Column(Float, nullable=True) # Preço de entrada para ordens a limite
    qty = Column(Float)
    stop_loss = Column(Float, nullable=True) # SL de entrada
    take_profit = Column(Float, nullable=True) # TP de entrada
    
    # Campos para gerenciamento
    entry_price = Column(Float, nullable=True) # Preço real de execução da ordem
    reduce_only = Column(Boolean, default=False) # True para ordens de fechamento
    position_idx = Column(Integer, nullable=True) # ID da posição para modificar
    new_stop_loss = Column(Float, nullable=True) # Para requisições de modificação
    new_take_profit = Column(Float, nullable=True) # Para requisições de modificação
    
    # Status pode ser: 'requested', 'close_requested', 'modify_requested', 
    # 'New', 'PartiallyFilled', 'Filled', 'Cancelled', 'failed'
    status = Column(String, index=True) 
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<Order(id='{self.client_order_id}', status='{self.status}', qty={self.qty})>"