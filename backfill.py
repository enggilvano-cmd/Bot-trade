import time
import argparse
from datetime import datetime, timedelta, timezone
from contextlib import closing
import yaml
import logging
# Correção FINALÍSSIMA: Usar pybit.http
from pybit.unified_trading import HTTP
from database.database import SessionLocal, init_db, engine # Importar engine
from database.models import Kline
from sqlalchemy.dialects.postgresql import insert as pg_insert # Importar especificamente para PostgreSQL
from sqlalchemy import inspect # Importar inspect
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_bybit_session():
    # API V5 (testnet=False para dados públicos)
    return HTTP(testnet=False, recv_window=10000)

BACKFILL_RETRY_POLICY = {
    "wait": wait_exponential(multiplier=1, min=1, max=10),
    "stop": stop_after_attempt(5), # Mais tentativas para backfill, pois é menos crítico em tempo real
    "retry": retry_if_exception_type(Exception), # Retry on any exception for robustness
}

@retry(**BACKFILL_RETRY_POLICY)
def fetch_historical_data(session, symbol, timeframe, end_time_ms):
    """
    Busca um lote de 1000 velas da Bybit (API V5) terminando ANTES do end_time_ms.
    A API V5 retorna dados do mais novo para o mais antigo.
    """
    try:
        # Usar o método get_kline da API V5
        # O limite máximo é 1000 velas por requisição.
        response = session.get_kline(
            category="linear",
            symbol=symbol,
            interval=str(timeframe),
            limit=1000,
            end=end_time_ms
        )
        # Checar código de retorno V5 ('retCode')
        if response and response.get('retCode') == 0:
            # Pegar a lista de resultados V5
            return response.get('result', {}).get('list', [])
        else:
             error_code = response.get('retCode', 'N/A')
             error_msg = response.get('retMsg', 'Unknown API error')
             logging.error(f"Erro da API Bybit ao buscar klines: Code={error_code}, Msg='{error_msg}' | Response: {response}")

    except Exception as e:
        logging.error(f"Exceção ao buscar dados da Bybit: {e}", exc_info=True)
    return [] # Retornar lista vazia em caso de erro

def run_backfill(symbol: str, timeframe: int, days_to_fetch: int):
    logging.info("--- Iniciando Backfill de Dados Históricos ---")
    logging.info(f"Configuração: Buscando {days_to_fetch} dias de dados para {symbol} em {timeframe}m.")
    
    try:
        init_db()
        logging.info("Banco de dados verificado/inicializado.")
    except Exception as e:
        logging.critical(f"Falha ao inicializar DB: {e}", exc_info=True)
        return

    # --- [VERIFICAÇÃO DE INTEGRIDADE DO SCHEMA] ---
    # Verifica se a constraint de unicidade existe na tabela 'klines'.
    # Este é um passo crucial para garantir que o 'ON CONFLICT' funcione.
    try:
        inspector = inspect(engine)
        constraints = inspector.get_unique_constraints('klines')
        constraint_found = any(
            c['name'] == '_symbol_timestamp_uc' and set(c['column_names']) == {'symbol', 'timestamp'}
            for c in constraints
        )
        if not constraint_found:
            logging.critical("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            logging.critical("!!! ERRO DE SCHEMA: A restrição de unicidade ('_symbol_timestamp_uc') não foi encontrada na tabela 'klines'.")
            logging.critical("!!! O 'ON CONFLICT' irá falhar. Para corrigir, apague a tabela 'klines' do seu banco de dados e reinicie o backfill.")
            logging.critical("!!! Exemplo de SQL (PostgreSQL): DROP TABLE klines;")
            logging.critical("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            return
    except Exception as e:
        logging.warning(f"Não foi possível verificar a constraint da tabela 'klines'. Pode falhar se a tabela não existir ainda. Erro: {e}")

    session = get_bybit_session()
    
    with closing(SessionLocal()) as db_session:
        # Define o período de tempo para o backfill
        # Começa do agora e vai para o passado
        end_of_period = datetime.now(timezone.utc)
        start_of_period = end_of_period - timedelta(days=days_to_fetch)
        
        # A busca é feita de trás para frente, então começamos com o timestamp atual
        current_end_ms = int(end_of_period.timestamp() * 1000)

        total_klines_saved = 0

        while True:
            current_end_dt = datetime.fromtimestamp(current_end_ms / 1000, tz=timezone.utc)
            if current_end_dt < start_of_period:
                logging.info("Período de backfill alvo alcançado.")
                break

            logging.info(f"Buscando lote de dados terminando em: {current_end_dt.isoformat()}")
            klines_v5_format = fetch_historical_data(session, symbol, timeframe, current_end_ms)

            if not klines_v5_format:
                logging.warning("Não foram recebidos dados neste lote. Fim do backfill ou erro da API.")
                break

            klines_as_dicts = []
            # A API retorna do mais novo para o mais antigo, então o primeiro item é o mais recente
            oldest_kline_time_ms_in_batch = int(klines_v5_format[-1][0])
            
            for k in klines_v5_format:
                try:
                    if not isinstance(k, list) or len(k) < 6:
                        logging.warning(f"Vela com formato inesperado, ignorando: {k!r}")
                        continue

                    kline_time_ms = int(k[0])
                    kline_time = datetime.fromtimestamp(kline_time_ms / 1000, tz=timezone.utc)

                    # [MELHORIA] Processar diretamente para dicionário, otimizando para a inserção em lote.
                    # Isso evita a criação de objetos SQLAlchemy intermediários.
                    klines_as_dicts.append({
                        "symbol": symbol, "timestamp": kline_time, "open": float(k[1]),
                        "high": float(k[2]), "low": float(k[3]), "close": float(k[4]),
                        "volume": float(k[5])
                    })
                except (IndexError, ValueError, TypeError) as parse_err:
                     logging.error(f"Erro ao processar dado da kline {k!r}: {parse_err}")

            # --- [MELHORIA DE PERFORMANCE] Inserção em lote com tratamento de conflitos (PostgreSQL) ---
            # Usa 'ON CONFLICT DO NOTHING' para ignorar duplicatas de forma eficiente no nível do DB.
            klines_saved_in_batch = 0
            if klines_as_dicts:
                try:
                    stmt = pg_insert(Kline).values(klines_as_dicts)
                    # [CORREÇÃO DEFINITIVA] Especificar o NOME da constraint para garantir que o PostgreSQL a encontre.
                    # O nome '_symbol_timestamp_uc' foi definido em database/models.py.
                    stmt = stmt.on_conflict_do_nothing(constraint='_symbol_timestamp_uc')
                    
                    result = db_session.execute(stmt)
                    db_session.commit()
                    klines_saved_in_batch = result.rowcount
                except Exception as e:
                    logging.error(f"Erro ao salvar lote de klines com on_conflict: {e}", exc_info=True)
                    db_session.rollback()
            
            total_klines_saved += klines_saved_in_batch
            logging.info(f"Salvas {klines_saved_in_batch} novas velas neste lote. (Total: {total_klines_saved})")
            
            # Prepara para o próximo lote, buscando dados ANTERIORES ao lote atual
            current_end_ms = oldest_kline_time_ms_in_batch
            time.sleep(0.5) # Cortesia para a API

        logging.info(f"--- Backfill concluído! Total de {total_klines_saved} velas novas salvas. ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bybit Historical Data Backfiller")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Trading symbol (e.g., BTCUSDT)")
    parser.add_argument("--timeframe", type=int, default=15, help="Candle timeframe in minutes (e.g., 1, 5, 15, 60)")
    parser.add_argument("--days", type=int, default=90, help="Number of past days to fetch data for")
    args = parser.parse_args()

    run_backfill(symbol=args.symbol, timeframe=args.timeframe, days_to_fetch=args.days)