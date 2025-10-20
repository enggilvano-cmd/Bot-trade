import time
from datetime import datetime, timedelta, timezone
import yaml
import logging
# Correção FINALÍSSIMA: Usar pybit.http
from pybit.unified_trading import HTTP
from database.database import SessionLocal, init_db
from database.models import Kline
from sqlalchemy.exc import IntegrityError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURAÇÕES ---
SYMBOL = "BTCUSDT"
TIMEFRAME = 15 # Intervalo em minutos
# ---------------------

def get_bybit_session():
    # API V5 (testnet=False para dados públicos)
    return HTTP(testnet=False, recv_window=10000)

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

def run_backfill():
    logging.info("--- Iniciando Backfill de Dados Históricos ---")

    with open('configs/btc_usdt_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    days_to_fetch = config.get('engine_params', {}).get('days_to_backfill', 365)
    logging.info(f"Configuração: Buscando {days_to_fetch} dias de dados para {SYMBOL} em {TIMEFRAME}m.")

    try:
        init_db()
        logging.info("Banco de dados verificado/inicializado.")
    except Exception as e:
        logging.critical(f"Falha ao inicializar DB: {e}", exc_info=True)
        return

    session = get_bybit_session()
    db_session = SessionLocal()

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
        klines_v5_format = fetch_historical_data(session, SYMBOL, TIMEFRAME, current_end_ms)

        if not klines_v5_format:
            logging.warning("Não foram recebidos dados neste lote. Fim do backfill ou erro da API.")
            break

        klines_to_add = []
        # A API retorna do mais novo para o mais antigo, então o primeiro item é o mais recente
        oldest_kline_time_ms_in_batch = int(klines_v5_format[-1][0])
        
        for k in klines_v5_format:
            try:
                kline_time_ms = int(k[0])
                kline_time = datetime.fromtimestamp(kline_time_ms / 1000, tz=timezone.utc)

                klines_to_add.append(Kline(
                    symbol=SYMBOL, timestamp=kline_time.replace(tzinfo=None),
                    open=float(k[1]), high=float(k[2]), low=float(k[3]),
                    close=float(k[4]), volume=float(k[5])
                ))
            except (IndexError, ValueError, TypeError) as parse_err:
                 logging.error(f"Erro ao processar dado da kline {k}: {parse_err}")

        try:
            # Inserção em lote é muito mais rápida
            # Usar um dicionário explícito é mais seguro do que usar __dict__
            # que pode conter atributos internos do SQLAlchemy como '_sa_instance_state'.
            kline_mappings = [
                {"symbol": k.symbol, "timestamp": k.timestamp, "open": k.open,
                 "high": k.high, "low": k.low, "close": k.close, "volume": k.volume}
                for k in klines_to_add
            ]
            db_session.bulk_insert_mappings(Kline, kline_mappings)
            db_session.commit()
            total_klines_saved += len(klines_to_add)
            logging.info(f"Salvas {len(klines_to_add)} novas velas.")
        except IntegrityError:
            db_session.rollback()
            logging.warning("Lote continha dados duplicados (IntegrityError). Isso é esperado ao sobrepor períodos. Continuando...")
        except Exception as e:
            logging.error(f"Erro geral ao salvar no banco: {e}", exc_info=True)
            db_session.rollback()
        
        # Prepara para o próximo lote, buscando dados ANTERIORES ao lote atual
        current_end_ms = oldest_kline_time_ms_in_batch
        time.sleep(0.5) # Cortesia para a API

    logging.info(f"--- Backfill concluído! Total de {total_klines_saved} velas novas salvas. ---")
    db_session.close()

if __name__ == "__main__":
    run_backfill()