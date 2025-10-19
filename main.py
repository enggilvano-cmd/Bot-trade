import yaml
import time
import logging
import os
import redis
from multiprocessing import Process
from components.data_collector import DataCollector
from components.trading_engine import TradingEngine
from components.order_manager import OrderManager
from database.database import init_db
from components.telegram_alerter import TelegramAlerter # <-- dotenv removido

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- [MELHORIA A++] Configuração do Heartbeat ---
HEARTBEAT_TIMEOUT = 180  # 3 minutos (ex: 2x o tempo de vela + margem)
PROCESS_NAMES = ["DataCollector", "OrderManager", "TradingEngine"]
# -----------------------------------------------

def wait_for_service(client, service_name):
    """Espera um serviço (Redis/DB) ficar disponível."""
    max_retries = 10
    retry_delay = 5  # segundos
    for i in range(max_retries):
        try:
            if service_name == "Redis":
                client.ping()
            elif service_name == "Database":
                # A função init_db já tenta criar a conexão e tabelas
                client()
            logger.info(f"{service_name} está pronto para conexões.")
            return True
        except Exception as e:
            logger.warning(f"Aguardando {service_name}... Tentativa {i+1}/{max_retries}. Erro: {e}")
            time.sleep(retry_delay)
    return False

def get_redis_client():
    """Conecta ao Redis para verificação de heartbeat."""
    try:
        client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            decode_responses=True
        )
        # A verificação de conexão será feita pelo wait_for_service
        return client
    except (redis.exceptions.ConnectionError, ValueError) as e:
        # Não é mais um erro crítico aqui, pois vamos tentar novamente
        logger.warning(f"Erro inicial ao configurar cliente Redis: {e}")
        return None

def run_process(target_class, **kwargs):
    """Função genérica para rodar uma classe em um processo."""
    try:
        obj = target_class(**kwargs)
        obj.run()
    except Exception as e:
        logger.critical(f"Erro fatal no processo {target_class.__name__}: {e}", exc_info=True)
        try:
            TelegramAlerter().send_message(f"🚨 PROCESSO CRÍTICO FALHOU: {target_class.__name__}\nErro: {e}")
        except Exception as alert_e:
            logger.error(f"Falha ao enviar alerta de falha: {alert_e}")

if __name__ == "__main__":
    logger.info("Sistema de Trading [PID: %s] iniciando...", os.getpid())
    
    # --- [MELHORIA] Esperar pelos serviços ---
    if not wait_for_service(init_db, "Database"):
        logger.critical("Falha ao conectar ao banco de dados após múltiplas tentativas. Encerrando.")
        exit(1)

    redis_client = get_redis_client()
    if not redis_client or not wait_for_service(redis_client, "Redis"):
        logger.critical("Falha ao conectar ao Redis após múltiplas tentativas. Encerrando.")
        exit(1)
    # -----------------------------------------

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
    processes = []

    for target_class, kwargs in processes_to_start:
        process = Process(target=run_process, args=(target_class,), kwargs=kwargs)
        process.start()
        processes.append((process, target_class.__name__))
        logger.info(f"Processo {target_class.__name__} iniciado [PID: {process.pid}]")

    # --- [MELHORIA A++] Loop de monitoramento com Heartbeat ---
    try:
        while True:
            time.sleep(10)
            for i, (process, name) in enumerate(processes):
                is_alive = process.is_alive()
                is_stale = False
                
                if is_alive:
                    # Processo está vivo, checar se está trabalhando (heartbeat)
                    try:
                        last_heartbeat = redis_client.get(f"heartbeat:{name}")
                        if last_heartbeat:
                            seconds_since_heartbeat = time.time() - int(last_heartbeat)
                            if seconds_since_heartbeat > HEARTBEAT_TIMEOUT:
                                is_stale = True
                                logger.error(f"Processo {name} [PID: {process.pid}] está VIVO mas travado (heartbeat obsoleto: {seconds_since_heartbeat:.0f}s). Reiniciando...")
                        else:
                            # Se não houver heartbeat ainda (ex: processo recém-iniciado), espere
                            logger.warning(f"Processo {name} [PID: {process.pid}] está VIVO mas ainda não enviou heartbeat. Aguardando...")
                            
                    except Exception as e:
                        logger.error(f"Erro ao checar heartbeat do {name}: {e}")
                        is_stale = True # Força reinício em caso de falha no Redis

                if not is_alive or is_stale:
                    if is_alive and is_stale: # Processo zumbi
                        logger.warning(f"Processo zumbi {name} detectado. Terminando...")
                        process.terminate() # Força o término
                        process.join(timeout=5)
                    
                    log_msg = f"Processo {name} foi encontrado MORTO." if not is_alive else f"Processo {name} foi REINICIADO (zumbi)."
                    logger.error(log_msg)
                    alerter.send_message(f"⚠️ {log_msg}")
                    
                    target_class, kwargs = processes_to_start[i]
                    new_process = Process(target=run_process, args=(target_class,), kwargs=kwargs)
                    new_process.start()
                    processes[i] = (new_process, name)
                    logger.info(f"Processo {name} reiniciado [NOVO PID: {new_process.pid}]")

    except KeyboardInterrupt:
        logger.info("Sinal de desligamento (Ctrl+C) recebido. Encerrando processos...")
        for process, name in processes:
            logger.info(f"Encerrando {name}...")
            process.terminate()
            process.join()
        alerter.send_message("🛑 Sistema de Trading DESLIGADO.")
    except Exception as e:
        logger.critical(f"Erro fatal no orquestrador principal: {e}", exc_info=True)
        alerter.send_message("🚨 ERRO CRÍTICO no Orquestrador Principal. O sistema pode estar offline.")