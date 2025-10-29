import yaml
import time
import logging
import os
import signal
import redis
from multiprocessing import Process
from components.data_collector import DataCollector
from components.trading_engine import TradingEngine
from components.order_manager import OrderManager
from database.database import init_db
from components.telegram_alerter import TelegramAlerter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HEARTBEAT_TIMEOUT = 180
HEARTBEAT_GRACE_PERIOD = 30

def get_redis_client():
    try:
        client = redis.Redis(
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            decode_responses=True
        )
        client.ping()
        logger.info("Conexão com Redis estabelecida com sucesso.")
        return client
    except (redis.exceptions.ConnectionError, ValueError) as e:
        logger.critical(f"Falha ao conectar ao Redis: {e}", exc_info=True)
        return None

def run_process(target_class, **kwargs):
    try:
        obj = target_class(**kwargs)
        obj.run()
    except Exception as e:
        logger.critical(f"Erro fatal no processo {target_class.__name__}: {e}", exc_info=True)
        try:
            TelegramAlerter().send_message(f"🚨 PROCESSO CRÍTICO FALHOU: {target_class.__name__}\nErro: {e}")
            exit(1) 
        except Exception as alert_e:
            logger.error(f"Falha ao enviar alerta de falha: {alert_e}")
            exit(1)

if __name__ == "__main__":
    logger.info("Sistema de Trading [PID: %s] iniciando...", os.getpid())
    
    if os.getenv("DATABASE_URL", "").startswith("sqlite") and os.getenv("LIVE_MODE", "False").lower() == "true":
        logger.critical("ERRO CRÍTICO: SQLite não é permitido em modo LIVE. Use PostgreSQL.")
        TelegramAlerter().send_message("🚨 ERRO CRÍTICO: SQLite não é permitido em modo LIVE. Encerrando.")
        exit(1)

    try:
        init_db()
        logger.info("Banco de dados inicializado/verificado.")
    except Exception as e:
        logger.critical(f"Falha ao conectar ao banco de dados: {e}", exc_info=True)
        exit(1)

    redis_client = get_redis_client()
    if not redis_client:
        logger.critical("Falha ao conectar ao Redis. Encerrando.")
        exit(1)

    with open('configs/btc_usdt_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    testnet = not config.get('live_mode', False)
    symbol = config['symbol']
    timeframe = str(config['timeframe'])

    alerter = TelegramAlerter()
    
    mode_msg = "TESTNET" if testnet else "LIVE"
    if config.get('shadow_mode', False) and config.get('live_mode', False):
        mode_msg = "LIVE (SHADOW MODE)"
        
    alerter.send_message(f"🚀 Sistema de Trading INICIADO\nModo: {mode_msg}\nSímbolo: {symbol}")

    processes_to_start = [
        (DataCollector, {"symbol": symbol, "timeframe": timeframe, "testnet": testnet}),
        (OrderManager, {"symbol": symbol, "testnet": testnet}),
        (TradingEngine, {"config": config, "alerter": alerter}),
    ]
    
    processes_state = {}
    MAX_FAILURES_BEFORE_HALT = 5
    FAILURE_RESET_INTERVAL = 300

    for target_class, kwargs in processes_to_start:
        name = target_class.__name__
        process = Process(target=run_process, args=(target_class,), kwargs=kwargs, name=name)
        process.start()
        
        processes_state[name] = {
            "process": process,
            "start_time": time.time(),
            "start_args": (target_class, kwargs),
            "failure_count": 0
        }
        logger.info(f"Processo {name} iniciado [PID: {process.pid}]")

    try:
        while True:
            time.sleep(10)
            
            for name, state in processes_state.items():
                if state["process"] is None:
                    continue

                process = state["process"]
                start_time = state["start_time"]
                is_alive = process.is_alive()

                if is_alive and state["failure_count"] > 0 and (time.time() - start_time) > FAILURE_RESET_INTERVAL:
                    logger.info(f"Processo {name} está estável. Resetando contagem de falhas.")
                    state["failure_count"] = 0

                is_stale = False
                if is_alive:
                    if (time.time() - start_time) > HEARTBEAT_GRACE_PERIOD:
                        try:
                            last_heartbeat = redis_client.get(f"heartbeat:{name}")
                            if last_heartbeat:
                                last_hb_time = int(last_heartbeat)
                                seconds_since_heartbeat = time.time() - last_hb_time
                                if seconds_since_heartbeat > HEARTBEAT_TIMEOUT:
                                    is_stale = True
                                    logger.error(f"Processo {name} [PID: {process.pid}] VIVO mas travado (heartbeat obsoleto: {seconds_since_heartbeat:.0f}s).")
                            else:
                                logger.warning(f"Processo {name} [PID: {process.pid}] VIVO mas sem heartbeat após grace period ({HEARTBEAT_GRACE_PERIOD}s).")
                                is_stale = True
                        except Exception as e:
                            logger.error(f"Erro ao checar heartbeat do {name}: {e}. Forçando reinício.")
                            is_stale = True

                if not is_alive or is_stale:
                    state["failure_count"] += 1
                    
                    if state["failure_count"] > MAX_FAILURES_BEFORE_HALT:
                        logger.critical(f"Processo {name} falhou {state['failure_count']} vezes. Parando de tentar reiniciar.")
                        alerter.send_message(f"🚨 CRÍTICO: Processo {name} falhou repetidamente e não será mais reiniciado. Intervenção manual necessária.")
                        state["process"] = None 
                        continue

                    if is_alive and is_stale:
                        logger.warning(f"Processo zumbi {name} [PID: {process.pid}] detectado. Encerrando...")
                        os.kill(process.pid, signal.SIGINT)
                        process.join(timeout=10)
                        if process.is_alive():
                            logger.warning(f"SIGINT falhou para {name}. Forçando SIGTERM...")
                            process.terminate()
                        process.join(timeout=5)
                    
                    log_msg = f"Processo {name} MORTO." if not is_alive else f"Processo {name} travado (zumbi)."
                    logger.error(log_msg)
                    alerter.send_message(f"⚠️ {log_msg}. Tentando reiniciar ({state['failure_count']}/{MAX_FAILURES_BEFORE_HALT}).")
                    
                    target_class, kwargs = state["start_args"]
                    new_process = Process(target=run_process, args=(target_class,), kwargs=kwargs, name=name)
                    new_process.start()
                    
                    state["process"] = new_process
                    state["start_time"] = time.time()
                    
                    logger.info(f"Processo {name} reiniciado [NOVO PID: {new_process.pid}]")

    except KeyboardInterrupt:
        logger.info("Sinal de desligamento (Ctrl+C) recebido. Encerrando processos...")
        
        for name, state in processes_state.items():
            if state["process"] is None:
                continue
            
            process = state["process"]
            logger.info(f"Encerrando processo {name} [PID: {process.pid}] (graciosamente)...")
            
            try:
                os.kill(process.pid, signal.SIGINT)
                process.join(timeout=10)
                
                if process.is_alive():
                    logger.warning(f"{name} não respondeu ao SIGINT. Enviando SIGTERM...")
                    process.terminate()
                    process.join(timeout=5)
            except ProcessLookupError:
                 logger.warning(f"Processo {name} [PID: {process.pid}] já não existia ao tentar encerrar.")
            except Exception as e:
                 logger.error(f"Erro ao encerrar processo {name}: {e}")

        logger.info("Todos os processos filhos encerrados.")
        alerter.send_message("🛑 Sistema de Trading DESLIGADO.")
        
    except Exception as e:
        logger.critical(f"Erro fatal no orquestrador principal: {e}", exc_info=True)
        alerter.send_message("🚨 ERRO CRÍTICO no Orquestrador Principal. O sistema pode estar offline.")