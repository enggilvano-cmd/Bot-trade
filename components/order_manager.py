import os
import time
import logging
import json
import redis
import uuid
# Correção: Usar os módulos corretos da pybit v5
from pybit.unified_trading import HTTP
from pybit.unified_trading import WebSocket
from pybit.exceptions import InvalidRequestError, FailedRequestError
from database.database import SessionLocal
from database.models import Order
from sqlalchemy import select, or_
from sqlalchemy.exc import IntegrityError # Importar IntegrityError
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception, retry_if_exception_type

logger = logging.getLogger(__name__)
NEW_ORDER_CHANNEL = "orders:new"
MODIFY_ORDER_CHANNEL = "orders:modify"
CLOSE_ORDER_CHANNEL = "orders:close"
ORDER_UPDATE_CHANNEL = "orders:update"

# Códigos de erro V5 não retentáveis (erros de lógica/parâmetro/saldo)
NON_RETRYABLE_ERROR_CODES = [
    10001, # Parâmetros inválidos genérico
    10002, # Logic error (ex: order not found for modification)
    10004, # Invalid API key or permissions
    10005, # Invalid signature
    110001, # category required
    110006, # qty required
    110007, # price required for limit order
    110012, # qty too low/high
    110013, # price too low/high
    110017, # Order not found or already filled/cancelled
    110020, # Insufficient balance
    110043, # SL/TP cannot be set (e.g., same as current, position size zero)
]

def is_retryable_api_error(exception):
    """Verifica se uma exceção da API Bybit (pybit v5+) deve ser retentada."""
    # (Código inalterado)
    if isinstance(exception, FailedRequestError):
        logger.warning(f"Erro 5xx da API ({exception.status_code}): {exception.message}. Tentando novamente...")
        return True
    if isinstance(exception, InvalidRequestError):
        ret_code = getattr(exception, 'ret_code', None)
        status_code = getattr(exception, 'status_code', None)
        if ret_code == 10006 or status_code == 429: # Rate limit
            logger.warning(f"Rate limit atingido (Code={ret_code}, Status={status_code}). Tentando novamente...")
            return True
        if ret_code in NON_RETRYABLE_ERROR_CODES:
             logger.error(f"Erro API NÃO RETENTÁVEL (Code={ret_code}): {exception.message}")
             return False
        logger.error(f"Erro API 4xx (Code={ret_code}, Status={status_code}): {exception.message}. NÃO TENTANDO NOVAMENTE.")
        return False
    logger.error(f"Exceção NÃO RETENTÁVEL encontrada: {type(exception).__name__} - {exception}")
    return False

# Política de retry atualizada
RETRY_POLICY = {
    "wait": wait_exponential(multiplier=1, min=1, max=10),
    "stop": stop_after_attempt(3),
    "retry": retry_if_exception(is_retryable_api_error),
}


class OrderManager:
    def __init__(self, symbol: str, testnet: bool = True):
        # (Código inalterado)
        self.symbol = symbol
        self.testnet = testnet
        self.api_key = os.getenv("BYBIT_API_KEY")
        self.api_secret = os.getenv("BYBIT_API_SECRET")
        if not self.api_key or not self.api_secret:
             logger.critical("API Key/Secret da Bybit não encontrados! OrderManager não funcionará.")
             raise ValueError("API Key/Secret não configurados.")
        try:
            self.redis_client = redis.Redis(
                host=os.getenv("REDIS_HOST", "localhost"), port=int(os.getenv("REDIS_PORT", 6379)),
                decode_responses=True, socket_connect_timeout=5, socket_timeout=5
            )
            self.redis_client.ping()
            logger.info("OrderManager conectado ao Redis.")
        except redis.exceptions.ConnectionError as e:
            logger.critical(f"OrderManager falhou ao conectar ao Redis: {e}")
            raise
        except Exception as e:
            logger.critical(f"OrderManager falhou ao conectar ao Redis (Erro Geral): {e}", exc_info=True)
            raise
        self.rest_session = self._connect_rest()
        self.ws_session = self._connect_ws()
        logger.info(f"OrderManager V5 para {symbol} inicializado.")

    def _connect_rest(self):
        # (Código inalterado)
        try:
            session = HTTP(
                testnet=self.testnet, api_key=self.api_key, api_secret=self.api_secret,
            )
            time_res = session.get_server_time()
            if time_res and time_res.get('retCode') == 0:
                 logger.info("Conexão REST Bybit V5 estabelecida.")
            else:
                 raise ConnectionError(f"Falha API V5 (get_server_time): {time_res}")
            return session
        except Exception as e:
            logger.critical(f"Falha CRÍTICA ao conectar API REST V5: {e}", exc_info=True)
            raise

    def _connect_ws(self):
        # (Código inalterado)
        try:
            ws = WebSocket(
                channel_type="private",
                testnet=self.testnet,
                api_key=self.api_key,
                api_secret=self.api_secret,
            )
            logger.info("Conexão WebSocket V5 Bybit estabelecida.")
            return ws
        except Exception as e:
            logger.critical(f"Falha CRÍTICA ao conectar WebSocket V5: {e}", exc_info=True)
            raise

    def _publish_update(self, client_order_id: str, status: str, order_id: str = None, 
                        avg_price: float = 0.0, error_msg: str = None, 
                        # --- [NÍVEL AVANÇADO] Enviar dados de estado atualizados ---
                        entry_price: float = 0.0, tp1_price: float = 0.0, is_tp1_hit: bool = False,
                        qty: float = 0.0):
        """Publica atualização no Redis."""
        try:
            update_data = {
                "client_order_id": client_order_id, "order_id": order_id, "status": status,
                "avg_price": avg_price, "error": error_msg,
                "entry_price": entry_price, "tp1_price": tp1_price,
                "is_tp1_hit": is_tp1_hit, "qty": qty
            }
            self.redis_client.publish(ORDER_UPDATE_CHANNEL, json.dumps(update_data))
            logger.debug(f"Update publicado CID {client_order_id}: Status={status}, Err={error_msg or 'N/A'}")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Redis Pub Error CID {client_order_id}: {e}")
        except Exception as e:
            logger.error(f"Erro inesperado Redis Pub CID {client_order_id}: {e}", exc_info=True)

    def _handle_order_update(self, msg):
        """Callback para WebSocket V5 'order'."""
        try:
            if isinstance(msg, dict) and msg.get('topic') == 'order' and 'data' in msg:
                for order_data in msg.get('data', []):
                    if not isinstance(order_data, dict) or order_data.get('symbol') != self.symbol:
                         continue

                    client_order_id = order_data.get('orderLinkId')
                    if not client_order_id or not client_order_id.startswith('bot_'):
                        continue

                    order_status = order_data.get('orderStatus')
                    order_id = order_data.get('orderId')
                    avg_price_str = order_data.get('avgPrice', "0")
                    avg_price = float(avg_price_str) if avg_price_str else 0.0
                    reject_reason = order_data.get('rejectReason', '')
                    order_qty_str = order_data.get('qty', '0')
                    order_qty = float(order_qty_str) if order_qty_str else 0.0

                    logger.info(f"WS V5 Update: CID={client_order_id}, OID={order_id}, Status={order_status}, AvgPx={avg_price_str}, Qty={order_qty}, RejRsn='{reject_reason}'")

                    with SessionLocal() as db_session:
                        try:
                            # Usar with_for_update() para lock pessimista
                            order = db_session.query(Order).filter(Order.client_order_id == client_order_id).with_for_update().first()
                            
                            if not order:
                                logger.critical(f"WS Update: ORDEM NÃO ENCONTRADA NO DB! CID={client_order_id}. Isso é um bug de sincronia em potencial.")
                                # Publicar mesmo assim para o TE tentar lidar (ex: via Deadlock)
                                self._publish_update(client_order_id, order_status, order_id, avg_price, reject_reason, qty=order_qty)
                                continue

                            # --- [NÍVEL AVANÇADO] Variáveis de estado para publicar ---
                            entry_price_to_pub = order.entry_price
                            tp1_price_to_pub = order.tp1_price
                            is_tp1_hit_to_pub = order.is_tp1_hit
                            # ----------------------------------------------------

                            # Mapear status V5 para status interno
                            new_status_internal = order_status
                            error_msg = None

                            if order_status == 'Filled':
                                new_status_internal = 'Filled'
                                # --- [NÍVEL AVANÇADO] Recalcular TP1 com base no fill real ---
                                # Apenas se for uma ordem de ABERTURA (não um fechamento)
                                if order.order_type == "Market" and not client_order_id.startswith("bot_close_"):
                                    if avg_price > 0 and order.tp1_rr and order.tp1_rr > 0:
                                        sl_dist = abs(avg_price - order.stop_loss)
                                        if order.side == "Buy":
                                            new_tp1_price = avg_price + (sl_dist * order.tp1_rr)
                                        else: # Sell
                                            new_tp1_price = avg_price - (sl_dist * order.tp1_rr)
                                        
                                        order.tp1_price = new_tp1_price
                                        order.entry_price = avg_price # Atualiza com o preço real
                                        
                                        entry_price_to_pub = order.entry_price
                                        tp1_price_to_pub = order.tp1_price
                                        logger.info(f"Fill de ABERTURA detectado. Preço Real: {avg_price:.2f}. Novo TP1 recalculado: {new_tp1_price:.2f}")
                                # ---------------------------------------------------------
                            
                            elif order_status == 'New': new_status_internal = 'New'
                            elif order_status == 'Cancelled': new_status_internal = 'Cancelled'
                            elif order_status == 'PartiallyFilled': new_status_internal = 'PartiallyFilled'
                            elif order_status == 'Rejected':
                                new_status_internal = 'Rejected'
                                error_msg = reject_reason
                            
                            if order.status in ['Filled', 'Cancelled', 'Rejected'] and new_status_internal != order.status:
                                 logger.warning(f"WS Update: Ignorando atualização de status {order.status} -> {new_status_internal} para CID={client_order_id}")
                                 continue

                            # Atualizar DB
                            order.status = new_status_internal
                            order.order_id = order_id
                            if avg_price > 0: order.avg_price = avg_price
                            if error_msg: order.error_message = error_msg
                            
                            db_session.commit()
                            
                            # Publicar atualização para o TradingEngine
                            self._publish_update(client_order_id, new_status_internal, order_id, 
                                                 avg_price, error_msg,
                                                 entry_price=entry_price_to_pub,
                                                 tp1_price=tp1_price_to_pub,
                                                 is_tp1_hit=is_tp1_hit_to_pub,
                                                 qty=order_qty)

                        except Exception as e_db:
                             db_session.rollback()
                             logger.error(f"Erro DB (handle_order_update) CID={client_order_id}: {e_db}", exc_info=True)

        except Exception as e:
            logger.error(f"Erro CRÍTICO _handle_order_update: {e} | Msg: {msg}", exc_info=True)


    # --- API Calls (com retry) ---

    @retry(**RETRY_POLICY)
    def _send_new_order_to_api(self, order: Order):
        """Envia a nova ordem para a API V5 (chamada retentável)."""
        logger.debug(f"API V5: Enviando place_order (CID={order.client_order_id})")
        
        # --- [NÍVEL AVANÇADO] Não enviar TP para ordens de abertura (será gerenciado pelo TE) ---
        # Apenas enviar SL. O TP será gerenciado manualmente ou em ordens de fechamento.
        tp_to_send = None
        if order.take_profit and order.take_profit > 0 and client_order_id.startswith("bot_close_"):
            # Permitir TP apenas para ordens de fechamento (se aplicável, embora raro)
            tp_to_send = str(order.take_profit)
        
        return self.rest_session.place_order(
            category="linear",
            symbol=self.symbol,
            side=order.side, # "Buy" ou "Sell"
            orderType=order.order_type, # "Market" ou "Limit"
            qty=str(order.qty),
            price=str(order.price) if order.order_type == "Limit" else None,
            orderLinkId=order.client_order_id,
            stopLoss=str(order.stop_loss) if order.stop_loss and order.stop_loss > 0 else None,
            takeProfit=tp_to_send, # <-- Modificado
            positionIdx=0 # Modo One-Way
        )

    @retry(**RETRY_POLICY)
    def _send_modify_order_to_api(self, mod_request: Order):
        """Envia a modificação SL/TP para a API V5 (chamada retentável)."""
        logger.debug(f"API V5: Enviando set_trading_stop (CID={mod_request.client_order_id})")
        
        # --- [NÍVEL AVANÇADO] Permitir cancelamento de TP (enviando "0") ---
        new_tp = None
        if mod_request.new_take_profit is not None:
             # Se for 0, envia "0". Se for > 0, envia string. Se for None, não envia.
             new_tp = str(mod_request.new_take_profit) 

        return self.rest_session.set_trading_stop(
            category="linear",
            symbol=self.symbol,
            positionIdx=mod_request.position_idx, # 0 para One-Way
            stopLoss=str(mod_request.new_stop_loss) if mod_request.new_stop_loss and mod_request.new_stop_loss > 0 else None,
            takeProfit=new_tp, # <-- Modificado
        )


    # --- Processamento de Requisições ---

    def _execute_new_order(self, order_req: Order):
        """Tenta executar uma nova ordem e publica o resultado."""
        try:
            # --- [FIX BUG 3] Salvar no DB ANTES de enviar para a API ---
            with SessionLocal() as db:
                try:
                    # Status inicial que indica que foi enviado ao DB, mas não à API
                    order_req.status = 'Submitted' 
                    db.add(order_req)
                    db.commit()
                except IntegrityError:
                     db.rollback()
                     logger.error(f"Erro de DB (IntegrityError): CID {order_req.client_order_id} já existe.")
                     self._publish_update(order_req.client_order_id, 'failed', error_msg="Duplicate CID")
                     return
                except Exception as e_db:
                     db.rollback()
                     logger.error(f"Erro de DB (pre-save) CID {order_req.client_order_id}: {e_db}", exc_info=True)
                     self._publish_update(order_req.client_order_id, 'failed', error_msg=f"DB Error: {e_db}")
                     return
            # -----------------------------------------------------------

            # Enviar para API (com retentativas)
            response = self._send_new_order_to_api(order_req)

            # Processar resposta da API
            if response and response.get('retCode') == 0:
                order_id = response.get('result', {}).get('orderId')
                logger.info(f"Ordem V5 enviada: CID={order_req.client_order_id}, OID={order_id}")
                
                # Atualizar DB com o OrderID da exchange
                with SessionLocal() as db:
                    order_db = db.query(Order).filter(Order.client_order_id == order_req.client_order_id).first()
                    if order_db:
                        order_db.order_id = order_id
                        # [FIX BUG 3] Mudar status para 'New' (enviado para exchange)
                        order_db.status = 'New'
                        db.commit()
                
            else:
                # (Código inalterado)
                code = response.get('retCode', 'N/A')
                msg = response.get('retMsg', 'Erro desconhecido da API')
                logger.error(f"Erro API V5 (place_order) FINAL: CID={order_req.client_order_id} Code={code} Msg='{msg}' | Resp: {response}")
                with SessionLocal() as db:
                     order_db = db.query(Order).filter(Order.client_order_id == order_req.client_order_id).first()
                     if order_db:
                         order_db.status = 'Rejected'
                         order_db.error_message = f"Code={code}: {msg}"
                         db.commit()
                self._publish_update(order_req.client_order_id, 'Rejected', error_msg=f"API Error Code={code}: {msg}")

        except (InvalidRequestError, FailedRequestError) as api_err:
             # (Código inalterado)
             code = getattr(api_err, 'ret_code', 'N/A')
             msg = getattr(api_err, 'message', str(api_err))
             logger.error(f"Erro API FINAL (place_order) CID={order_req.client_order_id}: Code={code} Msg='{msg}'", exc_info=False)
             with SessionLocal() as db:
                  order_db = db.query(Order).filter(Order.client_order_id == order_req.client_order_id).first()
                  if order_db:
                      order_db.status = 'failed'
                      order_db.error_message = f"API Retry Failed Code={code}: {msg}"
                      db.commit()
             self._publish_update(order_req.client_order_id, 'failed', error_msg=f"API Retry Failed Code={code}: {msg}")
             
        except Exception as e:
            # (Código inalterado)
            logger.error(f"Erro GERAL (execute_new_order) CID={order_req.client_order_id}: {e}", exc_info=True)
            with SessionLocal() as db:
                 order_db = db.query(Order).filter(Order.client_order_id == order_req.client_order_id).first()
                 if order_db and order_db.status != 'Rejected':
                     order_db.status = 'failed'
                     order_db.error_message = f"Internal Error: {e}"
                     db.commit()
            self._publish_update(order_req.client_order_id, 'failed', error_msg=f"Internal Error: {e}")

    def _execute_modify_order(self, mod_request: Order):
        """Envia modificação para API e publica resultado no Redis."""
        logger.info(f"Executando modificação SL/TP: CID={mod_request.client_order_id} posIdx={mod_request.position_idx} NewSL={mod_request.new_stop_loss} NewTP={mod_request.new_take_profit}")
        
        try:
            response = self._send_modify_order_to_api(mod_request)

            if response and response.get('retCode') == 0:
                logger.info(f"Modificação SL/TP CID={mod_request.client_order_id} enviada com sucesso.")
                self._publish_update(mod_request.client_order_id, 'Modified')
            
            else:
                # (Código inalterado)
                code = response.get('retCode', 'N/A')
                msg = response.get('retMsg', 'Erro API')
                if code == 110043:
                    logger.warning(f"Falha Modificar SL/TP (Code=110043): SL/TP igual ou inválido. CID={mod_request.client_order_id}. Resp: {response}")
                    self._publish_update(mod_request.client_order_id, 'failed', error_msg=f"API Warning Code={code}: {msg}")
                else:
                    logger.error(f"Erro API V5 (set_trading_stop): CID={mod_request.client_order_id} Code={code} Msg='{msg}' | Resp: {response}")
                    self._publish_update(mod_request.client_order_id, 'failed', error_msg=f"API Error Code={code}: {msg}")

        except (InvalidRequestError, FailedRequestError) as api_err:
             # (Código inalterado)
             code = getattr(api_err, 'ret_code', 'N/A')
             msg = getattr(api_err, 'message', str(api_err))
             logger.error(f"Erro API FINAL (set_trading_stop) CID={mod_request.client_order_id}: Code={code} Msg='{msg}'", exc_info=False)
             self._publish_update(mod_request.client_order_id, 'failed', error_msg=f"API Error Code={code}: {msg}")
             
        except Exception as e:
            # (Código inalterado)
            logger.error(f"Erro GERAL (execute_modify_order) CID={mod_request.client_order_id}: {e}", exc_info=True)
            self._publish_update(mod_request.client_order_id, 'failed', error_msg=f"Internal Error: {e}")


    # --- Redis Handlers ---

    def _on_new_order_request(self, message):
        cid = f'error_{uuid.uuid4()}' # Default CID in case of json error
        try:
            data = json.loads(message['data'])
            cid = data.get('client_order_id', f'new_{uuid.uuid4()}')
            logger.info(f"Recebida requisição NOVA ORDEM: CID={cid}")

            # --- [NÍVEL AVANÇADO] Capturar dados de estado ---
            order_req = Order(
                client_order_id=cid,
                symbol=data.get('symbol'),
                side=data.get('side'),
                order_type=data.get('order_type', 'Market'),
                qty=data.get('qty'),
                price=data.get('price'), # Usado para Limit
                stop_loss=data.get('stop_loss'),
                take_profit=data.get('take_profit'), # Geralmente None agora
                status='received', # Status interno antes de salvar no DB
                
                # Novos campos
                entry_price_estimate=data.get('entry_price_estimate'),
                tp1_price=data.get('tp1_price'),
                tp1_rr=data.get('tp1_rr')
            )
            # ------------------------------------------------
            
            self._execute_new_order(order_req)

        except json.JSONDecodeError:
            logger.error(f"JSON Decode Error (New Order): {message.get('data')}")
            self._publish_update(cid, 'failed', error_msg="Invalid JSON")
        except Exception as e:
            logger.error(f"Erro CRÍTICO _on_new_order_request (CID={cid}): {e}", exc_info=True)
            self._publish_update(cid, 'failed', error_msg=f"Internal Error: {e}")
        finally:
            try:
                self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))
            except Exception as e_hb:
                logger.error(f"OM Heartbeat Error (on_new): {e_hb}")


    def _on_modify_order_request(self, message):
        # (Código inalterado)
        cid = f'error_{uuid.uuid4()}'
        try:
            data = json.loads(message['data'])
            cid = data.get('client_order_id', f'mod_{uuid.uuid4()}')
            pos_idx = data.get('position_idx', 0)
            logger.info(f"Recebida requisição MODIFICAÇÃO SL/TP: CID={cid}, posIdx={pos_idx}")
            
            mod_req = Order( # Usar o objeto Order para passar os dados
                client_order_id=cid,
                symbol=data.get('symbol'),
                status='received',
                position_idx=pos_idx,
                new_stop_loss=data.get('new_stop_loss'),
                new_take_profit=data.get('new_take_profit')
            )
            
            self._execute_modify_order(mod_req)

        except json.JSONDecodeError:
            logger.error(f"JSON Decode Error (Modify Order): {message.get('data')}")
            self._publish_update(cid, 'failed', error_msg="Invalid JSON")
        except Exception as e:
            logger.error(f"Erro CRÍTICO _on_modify_order_request (CID={cid}): {e}", exc_info=True)
            self._publish_update(cid, 'failed', error_msg=f"Internal Error: {e}")
        finally:
            try:
                self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))
            except Exception as e_hb:
                logger.error(f"OM Heartbeat Error (on_modify): {e_hb}")


    def _on_close_order_request(self, message):
        # (Código inalterado)
        logger.warning(f"Recebida requisição CLOSE ORDER (NÃO IMPLEMENTADO): {message.get('data')}")
        try:
            self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))
        except Exception as e_hb:
             logger.error(f"OM Heartbeat Error (on_close, N/A): {e_hb}")


    def run(self):
        # (Código inalterado, incluindo lógica de reconexão do PubSub)
        try:
            self.ws_session.subscribe(["order"], callback=self._handle_order_update)
            logger.info(f"OM ouvindo WebSocket V5: ['order']")
        except Exception as e_ws:
             logger.critical(f"OM falha subscribe WebSocket V5: {e_ws}. Encerrando.", exc_info=True)
             return
        
        pubsub = None
        try:
            pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(**{
                NEW_ORDER_CHANNEL: self._on_new_order_request,
                MODIFY_ORDER_CHANNEL: self._on_modify_order_request,
                CLOSE_ORDER_CHANNEL: self._on_close_order_request
            })
            logger.info(f"OM ouvindo Redis: {list(pubsub.channels.keys())}")
        except redis.exceptions.ConnectionError as e:
             logger.critical(f"OM falha subscribe Redis: {e}. Encerrando."); return
        except Exception as e:
             logger.critical(f"OM erro subscribe Redis: {e}. Encerrando.", exc_info=True); return
        
        try:
            self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time())) # HB inicial
        except: pass

        while True:
            try:
                for message in pubsub.listen(): pass
                logger.critical("Loop pubsub.listen() do OrderManager terminou inesperadamente.")
                break
            except redis.exceptions.ConnectionError as e:
                 logger.critical(f"OM perdeu conexão Redis: {e}. Tentando reconectar pubsub...", exc_info=False)
                 try:
                     if pubsub: pubsub.close() 
                 except: pass
                 while True:
                     time.sleep(10)
                     try:
                         logger.info("Tentando reconectar PubSub Redis...")
                         pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)
                         pubsub.subscribe(**{
                             NEW_ORDER_CHANNEL: self._on_new_order_request,
                             MODIFY_ORDER_CHANNEL: self._on_modify_order_request,
                             CLOSE_ORDER_CHANNEL: self._on_close_order_request
                         })
                         logger.info("PubSub Redis reconectado!")
                         break
                     except redis.exceptions.ConnectionError:
                         logger.error("Falha ao reconectar PubSub Redis. Tentando novamente em 10s...")
                     except Exception as reconn_err:
                         logger.critical(f"Erro CRÍTICO ao tentar reconectar PubSub Redis: {reconn_err}. Encerrando OM.", exc_info=True)
                         return
            except KeyboardInterrupt:
                 logger.info("OrderManager recebendo sinal de interrupção...")
                 break
            except Exception as e:
                 logger.critical(f"Erro FATAL no loop listen() do OM: {e}. Encerrando.", exc_info=True)
                 break
        
        logger.info("OrderManager encerrando...")
        if pubsub:
            try: pubsub.close()
            except: pass
        if self.ws_session:
             try: self.ws_session.exit()
             except: pass