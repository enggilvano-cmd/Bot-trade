import time
import logging
import json
import redis
import os
from datetime import datetime, timezone
# Correção: Usar o WebSocket unificado da pybit v5
from pybit.unified_trading import WebSocket as UnifiedWebSocket
from database.database import SessionLocal
from database.models import Kline
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)
KLINE_CHANNEL = f"klines:{os.getenv('SYMBOL', 'BTCUSDT')}"

class DataCollector:
    def __init__(self, symbol: str, timeframe: str, testnet: bool = True):
        self.symbol = symbol
        self.timeframe_interval = str(timeframe)
        self.testnet = testnet

        try:
            self.redis_client = redis.Redis(
                host=os.getenv("REDIS_HOST", "localhost"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                decode_responses=True,
                socket_connect_timeout=5, socket_timeout=5
            )
            self.redis_client.ping()
            logger.info("DataCollector conectado ao Redis.")
        except redis.exceptions.ConnectionError as e:
            logger.critical(f"DataCollector falhou ao conectar ao Redis (ConnectionError): {e}")
            raise
        except Exception as e:
            logger.critical(f"DataCollector falhou ao conectar ao Redis (Outro Erro): {e}", exc_info=True)
            raise

        try:
             # Para streams públicos, o channel_type define a categoria (linear, spot, etc.)
             self.ws_session = UnifiedWebSocket(
                 channel_type="linear", # Para BTCUSDT
                 testnet=self.testnet
             )
             logger.info(f"DataCollector para {self.symbol} inicializado.")
        except Exception as e:
             logger.critical(f"Falha ao inicializar WebSocket V5: {e}", exc_info=True)
             raise

    def _handle_kline(self, msg):
        """Callback para novas velas do WebSocket (V5)."""
        try:
            if isinstance(msg, dict) and 'topic' in msg and msg['topic'].startswith('kline.') and 'data' in msg:
                for candle in msg.get('data', []):
                    if isinstance(candle, dict) and candle.get('confirm') is True:
                        start_ts_ms_str = candle.get('start')
                        open_p, high_p, low_p, close_p, volume_v, interval_v = (
                            candle.get('open'), candle.get('high'), candle.get('low'),
                            candle.get('close'), candle.get('volume'), candle.get('interval')
                        )

                        if not all([start_ts_ms_str, open_p, high_p, low_p, close_p, volume_v, interval_v]):
                             logger.warning(f"Mensagem kline V5 com dados faltando: {candle}")
                             continue

                        start_ts_ms = int(start_ts_ms_str)
                        kline_timestamp = datetime.fromtimestamp(start_ts_ms / 1000, tz=timezone.utc)

                        kline_obj = Kline(
                            symbol=self.symbol, timestamp=kline_timestamp.replace(tzinfo=None),
                            open=float(open_p), high=float(high_p), low=float(low_p),
                            close=float(close_p), volume=float(volume_v)
                        )

                        saved_to_db = False
                        with SessionLocal() as db_session:
                            try:
                                db_session.merge(kline_obj)
                                db_session.commit()
                                saved_to_db = True
                            except IntegrityError:
                                logger.warning(f"Dado duplicado (IntegrityError) {self.symbol} @ {kline_timestamp}, pulando.")
                                db_session.rollback()
                            except Exception as e:
                                logger.error(f"Erro ao salvar vela DB ({kline_timestamp}): {e}", exc_info=True)
                                db_session.rollback()

                        if saved_to_db:
                             try:
                                  candle_data = {
                                      "symbol": self.symbol, "timestamp": kline_timestamp.isoformat(),
                                      "open": float(open_p), "high": float(high_p), "low": float(low_p),
                                      "close": float(close_p), "volume": float(volume_v)
                                  }
                                  self.redis_client.publish(KLINE_CHANNEL, json.dumps(candle_data))
                                  logger.info(f"Vela {self.symbol} {interval_v}m @ {kline_timestamp.strftime('%Y-%m-%d %H:%M:%S')} salva/publicada.")
                                  self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))
                             except redis.exceptions.ConnectionError as redis_err:
                                  logger.error(f"Erro Redis (publish/heartbeat): {redis_err}")
                             except Exception as pub_e:
                                  logger.error(f"Erro ao publicar vela Redis: {pub_e}", exc_info=True)

            elif 'topic' not in msg or not msg['topic'].startswith('kline.'):
                 logger.debug(f"Mensagem WS (não kline data): {msg}")

        except Exception as e:
            logger.error(f"Erro CRÍTICO _handle_kline: {e} | Mensagem: {msg}", exc_info=True)

    def run(self):
        logger.info(f"Iniciando DataCollector WebSocket V5 para {self.symbol}...")
        try:
             # Usar o método kline_stream para se inscrever no tópico de velas
             self.ws_session.kline_stream(
                 interval=self.timeframe_interval,
                 symbol=self.symbol,
                 callback=self._handle_kline
             )
             logger.info(f"Inscrito no stream público WebSocket V5: kline.{self.timeframe_interval}.{self.symbol}")
        except Exception as e:
             logger.critical(f"Falha ao iniciar stream público WebSocket V5: {e}", exc_info=True)
             raise

        while True:
            try:
                # O stream roda em background, apenas enviar heartbeats
                self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))
            except redis.exceptions.ConnectionError as redis_err:
                 logger.error(f"DC Heartbeat: Erro conexão Redis: {redis_err}")
            except Exception as e:
                logger.error(f"DC Heartbeat: Erro: {e}", exc_info=True)
            time.sleep(30) # Heartbeat a cada 30s