import logging
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect
from database.database import SessionLocal, engine, init_db
from database.models import Base, Kline, Order

# Configura o logging básico
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clear_table_data():
    """
    Deleta todos os registros das tabelas 'klines' e 'orders'.
    Esta é uma forma segura de resetar os dados sem perder o schema (tabelas, índices, constraints).
    """
    session = SessionLocal()
    inspector = inspect(engine)
    try:
        logging.info("Iniciando a limpeza das tabelas 'orders' e 'klines'...")

        # Verifica se a tabela 'orders' existe antes de tentar deletar
        if inspector.has_table(Order.__tablename__):
            num_orders_deleted = session.query(Order).delete()
            logging.info(f"{num_orders_deleted} registros deletados da tabela 'orders'.")
        else:
            logging.info("Tabela 'orders' não encontrada, pulando a limpeza.")

        # Verifica se a tabela 'klines' existe antes de tentar deletar
        if inspector.has_table(Kline.__tablename__):
            num_klines_deleted = session.query(Kline).delete()
            logging.info(f"{num_klines_deleted} registros deletados da tabela 'klines'.")
        else:
            logging.info("Tabela 'klines' não encontrada, pulando a limpeza.")

        session.commit()
        logging.info("Limpeza do banco de dados concluída com sucesso.")

    except SQLAlchemyError as e:
        logging.error(f"Ocorreu um erro durante a limpeza do banco de dados: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()

def reset_database_schema():
    """
    APAGA COMPLETAMENTE as tabelas e as recria a partir dos modelos.
    Use isso quando houver mudanças no schema (ex: adicionar constraints).
    """
    try:
        logging.warning("Iniciando a recriação completa do schema do banco de dados...")
        logging.warning("APAGANDO todas as tabelas...")
        Base.metadata.drop_all(bind=engine)
        logging.info("Tabelas apagadas.")
        logging.info("Recriando tabelas a partir dos modelos...")
        init_db()
        logging.info("Schema do banco de dados recriado com sucesso.")
    except Exception as e:
        logging.error(f"Ocorreu um erro durante a recriação do schema: {e}", exc_info=True)

if __name__ == "__main__":
    action = input("Escolha a ação: [1] Limpar dados (manter schema) | [2] RESETAR SCHEMA (apagar e recriar tabelas): ")
    if action == '1':
        clear_table_data()
    elif action == '2':
        confirm = input("ATENÇÃO: Isso apagará TODAS as tabelas e dados. Tem certeza? (s/N): ")
        if confirm.lower() == 's':
            reset_database_schema()
    else:
        logging.info("Operação de limpeza cancelada pelo usuário.")