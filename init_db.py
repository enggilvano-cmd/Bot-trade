from database.database import init_db
import logging

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Inicializando o banco de dados...")
    try:
        init_db()
        logging.info("Banco de dados e tabelas criados com sucesso.")
    except Exception as e:
        logging.error(f"Falha ao inicializar o banco de dados: {e}", exc_info=True)