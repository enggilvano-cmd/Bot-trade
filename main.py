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

# --- Configuração do Heartbeat ---
HEARTBEAT_TIMEOUT = 180  # 3 minutos
HEARTBEAT_GRACE_PERIOD = 30 # 30 segundos de carência
# -----------------------------------------------

def get_redis_client():
    """Conecta ao Redis para verificação de heartbeat."""
    try:
        # [CORREÇÃO 1] O host padrão deve ser 'redis' (nome do serviço no Docker Compose)
        # 'localhost' falharia de dentro do container 'app'.
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
    """Função genérica para rodar uma classe em um processo."""
    try:
        obj = target_class(**kwargs)
        obj.run()
    except Exception as e:
        logger.critical(f"Erro fatal no processo {target_class.__name__}: {e}", exc_info=True)
        try:
            # Cria um alerter 'descartável' apenas para este erro fatal
            TelegramAlerter().send_message(f"🚨 PROCESSO CRÍTICO FALHOU: {target_class.__name__}\nErro: {e}")
            # Em caso de falha crítica, o processo deve sair para ser reiniciado pelo orquestrador.
            exit(1) 
        except Exception as alert_e:
            logger.error(f"Falha ao enviar alerta de falha: {alert_e}")
            exit(1) # Se nem o alerta funciona, sair.

if __name__ == "__main__":
    logger.info("Sistema de Trading [PID: %s] iniciando...", os.getpid())
    
    # --- Verificação de ambiente ---
    if os.getenv("DATABASE_URL", "").startswith("sqlite") and os.getenv("LIVE_MODE", "False").lower() == "true":
        logger.critical("ERRO CRÍTICO: SQLite não é permitido em modo LIVE. Use PostgreSQL.")
        TelegramAlerter().send_message("🚨 ERRO CRÍTICO: SQLite não é permitido em modo LIVE. Encerrando.")
        exit(1)
    # ---------------------------------------------

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
    
    # Modos de inicialização
    mode_msg = "TESTNET" if testnet else "LIVE"
    if config.get('shadow_mode', False) and config.get('live_mode', False):
        mode_msg = "LIVE (SHADOW MODE)"
        
    alerter.send_message(f"🚀 Sistema de Trading INICIADO\nModo: {mode_msg}\nSímbolo: {symbol}")

    processes_to_start = [
        (DataCollector, {"symbol": symbol, "timeframe": timeframe, "testnet": testnet}),
        (OrderManager, {"symbol": symbol, "testnet": testnet}),
        (TradingEngine, {"config": config, "alerter": alerter}),
    ]
    
    # [REATORAÇÃO 2] Centraliza o estado em um dicionário.
    # Isso é mais limpo do que gerenciar 'processes' e 'process_failure_counts' separadamente.
    processes_state = {}
    MAX_FAILURES_BEFORE_HALT = 5
    FAILURE_RESET_INTERVAL = 300 # 5 minutos

    for target_class, kwargs in processes_to_start:
        name = target_class.__name__
        process = Process(target=run_process, args=(target_class,), kwargs=kwargs, name=name)
        process.start()
        
        processes_state[name] = {
            "process": process,
            "start_time": time.time(),
            "start_args": (target_class, kwargs), # Armazena como reiniciar
            "failure_count": 0
        }
        logger.info(f"Processo {name} iniciado [PID: {process.pid}]")

    # --- Loop de monitoramento com Heartbeat ---
    try:
        while True:
            time.sleep(10)
            
            for name, state in processes_state.items():
                
                # Ignora processos que falharam permanentemente
                if state["process"] is None:
                    continue

                process = state["process"]
                start_time = state["start_time"]
                is_alive = process.is_alive()

                # Resetar contagem de falhas se o processo estiver estável por um tempo
                if is_alive and state["failure_count"] > 0 and (time.time() - start_time) > FAILURE_RESET_INTERVAL:
                    logger.info(f"Processo {name} está estável. Resetando contagem de falhas.")
                    state["failure_count"] = 0

                is_stale = False
                
                if is_alive:
                    # Only check heartbeat if grace period has passed
                    if (time.time() - start_time) > HEARTBEAT_GRACE_PERIOD:
                        try:
                            last_heartbeat = redis_client.get(f"heartbeat:{name}")
                            if last_heartbeat:
                                last_hb_time = int(last_heartbeat)
                                seconds_since_heartbeat = time.time() - last_hb_time
                                if seconds_since_heartbeat > HEARTBEAT_TIMEOUT:
                                    is_stale = True
                                    logger.error(f"Processo {name} [PID: {process.pid}] está VIVO mas travado (heartbeat obsoleto: {seconds_since_heartbeat:.0f}s). Reiniciando...")
                            else:
                                logger.warning(f"Processo {name} [PID: {process.pid}] está VIVO mas ainda não enviou heartbeat após grace period ({HEARTBEAT_GRACE_PERIOD}s). Reiniciando...")
                                is_stale = True # Treat as stale if no heartbeat after grace
                        except Exception as e:
                            logger.error(f"Erro ao checar heartbeat do {name}: {e}. Forçando reinício.")
                            is_stale = True # Força reinício em caso de falha no Redis
                    
                    else:
                        logger.debug(f"Processo {name} [PID: {process.pid}] está dentro do grace period. (Tempo decorrido: {time.time() - start_time:.0f}s)")

                if not is_alive or is_stale: # Processo morto ou travado
                    state["failure_count"] += 1
                    
                    if state["failure_count"] > MAX_FAILURES_BEFORE_HALT:
                        logger.critical(f"Processo {name} falhou {state['failure_count']} vezes. Parando de tentar reiniciar.")
                        alerter.send_message(f"🚨 CRÍTICO: Processo {name} falhou repetidamente e não será mais reiniciado. Intervenção manual necessária.")
                        # Marcar como inativo
                        state["process"] = None 
                        continue # Pular para o próximo processo

                    if is_alive and is_stale: # Processo zumbi
                        logger.warning(f"Processo zumbi {name} [PID: {process.pid}] detectado. Tentando encerrar...")
                        # Tenta um encerramento gracioso (SIGINT)
                        os.kill(process.pid, signal.SIGINT)
                        process.join(timeout=10) # Dá 10s para encerrar
                        if process.is_alive():
                            logger.warning(f"Encerramento gracioso (SIGINT) falhou para {name}. Forçando término (SIGTERM)...")
                            process.terminate() # Força o término (SIGTERM)
                        process.join(timeout=5)
                    
                    log_msg = f"Processo {name} foi encontrado MORTO." if not is_alive else f"Processo {name} travou (zumbi)."
                    logger.error(log_msg)
                    alerter.send_message(f"⚠️ {log_msg}. Tentando reiniciar ({state['failure_count']}/{MAX_FAILURES_BEFORE_HALT}).")
                    
                    new_start_time = time.time()
                    
                    # [REATORAÇÃO 2] Lógica de reinício simplificada
                    target_class, kwargs = state["start_args"]
                    
                    new_process = Process(target=run_process, args=(target_class,), kwargs=kwargs, name=name)
                    new_process.start()
                    
                    # Atualiza o estado
                    state["process"] = new_process
                    state["start_time"] = new_start_time
                    
                    logger.info(f"Processo {name} reiniciado [NOVO PID: {new_process.pid}]")

    except KeyboardInterrupt:
        logger.info("Sinal de desligamento (Ctrl+C) recebido. Encerrando processos...")
        
        # [CORREÇÃO 3] Lógica de desligamento quebrada e melhorada
        for name, state in processes_state.items():
            if state["process"] is None:
                continue
            
            process = state["process"]
            logger.info(f"Encerrando processo {name} [PID: {process.pid}] (graciosamente)...")
            
            try:
                # Tenta SIGINT (gracioso) primeiro, assim como o zumbi killer
                os.kill(process.pid, signal.SIGINT)
                process.join(timeout=10)
                
                if process.is_alive():
                    logger.warning(f"{name} não respondeu ao SIGINT. Enviando SIGTERM...")
                    process.terminate() # Envia SIGTERM
                    process.join(timeout=5)
            except ProcessLookupError:
                 logger.warning(f"Processo {name} [PID: {process.pid}] já não existia ao tentar encerrar.")
            except Exception as e:
                 logger.error(f"Erro ao encerrar processo {name}: {e}")

        logger.info("Todos os processos filhos encerrados.")
        alerter.send_message("🛑 Sistema de Trading DESLIGADO.")
        
    except Exception as e:
        logger.critical(f"Erro fatal no orquestrador principal: {e}", exc_info=True)
        # [CORREÇÃO 4] Usa a instância 'alerter' existente
        alerter.send_message("🚨 ERRO CRÍTICO no Orquestrador Principal. O sistema pode estar offline.")