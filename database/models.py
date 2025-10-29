import os
import logging
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, UniqueConstraint, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import func # <-- Importado
from datetime import datetime

logger = logging.getLogger(__name__)
Base = declarative_base()

class Kline(Base):
    __tablename__ = 'klines'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(20), nullable=False)
    
    # --- [CORREÇÃO] Usar DateTime(timezone=True) ---
    # Armazena como "TIMESTAMP WITH TIME ZONE" no PostgreSQL
    timestamp = Column(DateTime(timezone=True), nullable=False)
    # ----------------------------------------------
    
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)

    __table_args__ = (
        UniqueConstraint('symbol', 'timestamp', name='_symbol_timestamp_uc'),
        Index('ix_klines_symbol_timestamp', 'symbol', 'timestamp')
    )

    def __repr__(self):
        return f"<Kline(symbol='{self.symbol}', timestamp='{self.timestamp}', close='{self.close}')>"

class Order(Base):
    __tablename__ = 'orders'
    
    id = Column(Integer, primary_key=True)
    client_order_id = Column(String(64), nullable=False, unique=True, index=True)
    order_id = Column(String(64), unique=True, index=True, nullable=True) # OID da exchange
    symbol = Column(String(20), nullable=False, index=True)
    side = Column(String(10), nullable=False)
    order_type = Column(String(20), nullable=False)
    qty = Column(Float, nullable=False)
    price = Column(Float, nullable=True)
    stop_loss = Column(Float, nullable=True)
    take_profit = Column(Float, nullable=True)
    status = Column(String(20), nullable=False, index=True)
    entry_price = Column(Float, nullable=True)
    reduce_only = Column(Boolean, default=False)
    
    # --- [MELHORIA] Campos para modificação de SL/TP (via OrderManager) ---
    # Usado para rastrear o 'positionIdx' da Bybit (ex: 0 para modo unificado)
    position_idx = Column(Integer, nullable=True) 
    # Usado para solicitações de 'modify' que não criam uma nova ordem
    new_stop_loss = Column(Float, nullable=True)
    new_take_profit = Column(Float, nullable=True)
    # ----------------------------------------------------------------------
    
    # --- [CORREÇÃO] Usar DateTime(timezone=True) e server_default ---
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    # ----------------------------------------------------------------
    
    def __repr__(self):
        return f"<Order(cid='{self.client_order_id}', status='{self.status}', qty='{self.qty}')>"

def init_db(engine_instance):
    """Cria as tabelas no banco de dados."""
    try:
        Base.metadata.create_all(engine_instance)
        logger.info("Tabelas do banco de dados verificadas/criadas com sucesso.")
    except Exception as e:
        logger.critical(f"Falha ao criar tabelas do banco de dados: {e}", exc_info=True)
        raise

# (O restante do arquivo database.py, que busca a URL do DB, permanece o mesmo)
# Esta função 'init_db' está em 'database.py' no seu projeto original, 
# mas a definição dos modelos está aqui.