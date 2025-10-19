import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Usa uma variável de ambiente. Se não existir, usa um arquivo SQLite local.
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./trading_system.db")

# O check_same_thread=False é necessário APENAS para SQLite
engine_args = {}
if DATABASE_URL.startswith("sqlite"):
    engine_args["connect_args"] = {"check_same_thread": False}
    print("AVISO: Usando SQLite. Não recomendado para produção com múltiplos processos.")
elif DATABASE_URL.startswith("postgresql"):
     print("Usando PostgreSQL.")

engine = create_engine(DATABASE_URL, **engine_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    from database.models import Base
    Base.metadata.create_all(bind=engine)