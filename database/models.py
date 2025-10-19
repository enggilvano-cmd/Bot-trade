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
    order_id = Column(String, unique=True, index=True, nullable=True) # ID da Exchange (pode ser nulo no início)
    
    symbol = Column(String, index=True)
    side = Column(String)
    order_type = Column(String)
    price = Column(Float, nullable=True) # Preço de entrada (para ordens Limit)
    qty = Column(Float)
    
    stop_loss = Column(Float, nullable=True)
    take_profit = Column(Float, nullable=True) # Take Profit total (se usado)

    status = Column(String, index=True, default='Pending') # Ex: Pending, New, Filled, Cancelled, Rejected, Modified
    
    # [MELHORIA A++] Campos para modificação
    new_stop_loss = Column(Float, nullable=True)
    new_take_profit = Column(Float, nullable=True)
    position_idx = Column(Integer, nullable=True) # Para modificação de SL/TP V5
    
    avg_price = Column(Float, nullable=True) # Preço médio de execução
    error_message = Column(String, nullable=True) # Se 'Rejected' ou 'failed'

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # --- [IMPLEMENTAÇÃO NÍVEL AVANÇADO] Campos de Gerenciamento de Posição ---
    # Preço de entrada estimado (enviado pelo TE) vs avg_price (preço real de fill)
    entry_price_estimate = Column(Float, nullable=True) 
    # Preço do Take Profit Parcial (TP1)
    tp1_price = Column(Float, nullable=True) 
    # Flag para rastrear se o TP1 já foi atingido e processado
    is_tp1_hit = Column(Boolean, default=False, index=True)
    # Relação R/R do TP1 (para recálculo se o 'avg_price' for diferente do 'entry_price_estimate')
    tp1_rr = Column(Float, nullable=True)
    # -----------------------------------------------------------------------

    def __repr__(self):
        return f"<Order(cid='{self.client_order_id}', status='{self.status}', qty={self.qty})>"