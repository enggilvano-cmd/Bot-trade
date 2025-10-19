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
    # Adicionar outros códigos conforme documentação V5
]

def is_retryable_api_error(exception):
    """Verifica se uma exceção da API Bybit (pybit v5+) deve ser retentada."""
    # Erros 5xx geralmente são temporários
    if isinstance(exception, FailedRequestError):
        logger.warning(f"Erro 5xx da API ({exception.status_code}): {exception.message}. Tentando novamente...")
        return True
    # Erros 4xx (InvalidRequestError) - verificar código específico
    if isinstance(exception, InvalidRequestError):
        ret_code = getattr(exception, 'ret_code', None)
        status_code = getattr(exception, 'status_code', None)

        if ret_code == 10006 or status_code == 429: # Rate limit
            logger.warning(f"Rate limit atingido (Code={ret_code}, Status={status_code}). Tentando novamente...")
            return True
        # Códigos específicos que *podem* ser temporários (ex: bloqueio por IP?) - Adicionar se necessário
        # if ret_code == ALGUM_CODIGO_TEMPORARIO: return True

        # Não retentar erros lógicos/parâmetros/saldo
        if ret_code in NON_RETRYABLE_ERROR_CODES:
             logger.error(f"Erro API NÃO RETENTÁVEL (Code={ret_code}): {exception.message}")
             return False

        # Outros InvalidRequestError (4xx) - não retentar por padrão
        logger.error(f"Erro API 4xx (Code={ret_code}, Status={status_code}): {exception.message}. NÃO TENTANDO NOVAMENTE.")
        return False

    # Não retentar outros tipos de exceção (TypeError, ValueError, etc.)
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
        try:
            session = HTTP(
                testnet=self.testnet, api_key=self.api_key, api_secret=self.api_secret,
                # logging=True, log_requests=True # Para debug
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
        try:
            # A v5 requer o 'channel_type' na inicialização
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

    def _publish_update(self, client_order_id: str, status: str, order_id: str = None, avg_price: float = 0.0, error_msg: str = None):
        """Publica atualização no Redis."""
        try:
            update_data = {
                "client_order_id": client_order_id, "order_id": order_id, "status": status,
                "avg_price": avg_price, "error": error_msg
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

                    logger.info(f"WS V5 Update: CID={client_order_id}, OID={order_id}, Status={order_status}, AvgPx={avg_price_str}, RejRsn='{reject_reason}'")

                    with SessionLocal() as db_session:
                        try:
                            # Usar with_for_update() para lock pessimista
                            order = db_session.query(Order).filter(Order.client_order_id == client_order_id).with_for_update().first()
                            if order:
                                if order.status != order_status or order_status in ['Filled', 'Cancelled', 'Rejected', 'Expired']:
                                    order.status = order_status
                                    order.order_id = order_id
                                    if order_status == 'Filled' and avg_price > 0 and order.entry_price is None:
                                        order.entry_price = avg_price
                                    db_session.commit()
                                    logger.info(f"Ordem DB CID={client_order_id} atualizada -> {order_status}")

                                    if order_status in ['Filled', 'Cancelled', 'Rejected', 'Expired']:
                                        error_msg = reject_reason if order_status == 'Rejected' else None
                                        final_avg_price = order.entry_price if order.entry_price else avg_price
                                        self._publish_update(client_order_id, order_status, order_id, final_avg_price, error_msg)
                            else:
                                logger.warning(f"WS Update para ordem não encontrada no DB (CID={client_order_id}).")

                        except Exception as e:
                            logger.error(f"Erro CRÍTICO ao atualizar ordem DB (CID={client_order_id}): {e}", exc_info=True)
                            db_session.rollback()

            # Enviar heartbeat mesmo sem mensagens de ordem
            self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))

        except Exception as e:
            logger.error(f"Erro CRÍTICO no _handle_order_update: {e} | Mensagem: {msg}", exc_info=True)


    # --- API Calls V5 ---
    @retry(**RETRY_POLICY) # Usar a política com verificação
    def _send_new_order_to_api(self, order: Order):
        """Usa place_order da V5."""
        logger.debug(f"API V5 place_order: CID={order.client_order_id} Side={order.side} Qty={order.qty} SL={order.stop_loss} TP={order.take_profit}")
        sl_str = str(order.stop_loss) if order.stop_loss and order.stop_loss > 0 else None
        tp_str = str(order.take_profit) if order.take_profit and order.take_profit > 0 else None

        return self.rest_session.place_order(
            category="linear", symbol=order.symbol, side=order.side,
            orderType=order.order_type, qty=str(order.qty),
            price=str(order.price) if order.order_type == 'Limit' and order.price else None,
            timeInForce="GoodTillCancel", reduceOnly=False, closeOnTrigger=False,
            orderLinkId=order.client_order_id, stopLoss=sl_str, takeProfit=tp_str,
            # positionIdx=0
        )

    def _execute_new_order(self, order: Order):
        """Salva, envia, atualiza DB."""
        logger.info(f"Executando nova ordem: CID={order.client_order_id} Side={order.side} Qty={order.qty}")
        order_in_db = None
        with SessionLocal() as db_session:
            try:
                existing = db_session.query(Order).filter(Order.client_order_id == order.client_order_id).first()
                if existing:
                     logger.warning(f"Ordem CID={order.client_order_id} já existe (Status: {existing.status}). Ignorando.")
                     return

                order.status = 'pending_submission'
                db_session.add(order)
                db_session.commit()
                order_in_db = order

                response = self._send_new_order_to_api(order)

                if response and response.get('retCode') == 0:
                    result = response.get('result', {})
                    oid_api = result.get('orderId')
                    order_to_update = db_session.query(Order).filter(Order.client_order_id == order.client_order_id).first()
                    if order_to_update:
                         order_to_update.order_id = oid_api
                         order_to_update.status = 'Submitted'
                         db_session.commit()
                         logger.info(f"Ordem CID={order.client_order_id} enviada V5. OID: {oid_api}")
                    else: logger.error(f"Ordem CID={order.client_order_id} sumiu do DB após envio API!")
                else:
                    code = response.get('retCode', 'N/A')
                    msg = response.get('retMsg', 'Erro API desconhecido')
                    logger.error(f"Erro API V5 (place_order): CID={order.client_order_id} Code={code} Msg='{msg}' | Resp: {response}")
                    order_to_update = db_session.query(Order).filter(Order.client_order_id == order.client_order_id).first()
                    if order_to_update:
                         order_to_update.status = 'failed'
                         db_session.commit()
                    self._publish_update(order.client_order_id, 'failed', error_msg=f"API Error Code={code}: {msg}")

            except (InvalidRequestError, FailedRequestError) as api_err:
                 code = getattr(api_err, 'ret_code', 'N/A')
                 msg = getattr(api_err, 'message', str(api_err))
                 logger.error(f"Erro API FINAL (place_order) CID={order.client_order_id}: Code={code} Msg='{msg}'", exc_info=False)
                 if db_session.is_active:
                     try:
                        order_to_update = db_session.query(Order).filter(Order.client_order_id == order.client_order_id).first()
                        if order_to_update and order_to_update.status == 'pending_submission': # Só marcar falha se ainda estava pendente
                             order_to_update.status = 'failed'
                             db_session.commit()
                        else: db_session.rollback() # Ordem não encontrada ou já processada
                     except Exception as db_err:
                          logger.error(f"Erro DB ao marcar falha API (place_order) CID={order.client_order_id}: {db_err}", exc_info=True)
                          db_session.rollback()
                 else: logger.error("Sessão DB inativa pós-erro API (place_order)")
                 self._publish_update(order.client_order_id, 'failed', error_msg=f"API Error Code={code}: {msg}")

            except Exception as e:
                logger.error(f"Erro GERAL (execute_new_order) CID={order.client_order_id}: {e}", exc_info=True)
                if db_session.is_active: db_session.rollback()
                self._publish_update(order.client_order_id or f"unknown_{uuid.uuid4()}", 'failed', error_msg=f"Internal Error: {e}")

    @retry(**RETRY_POLICY)
    def _send_close_order_to_api(self, closing_order: Order):
        """Usa place_order V5 com reduceOnly=True."""
        logger.debug(f"API V5 place_order (close): CID={closing_order.client_order_id} Side={closing_order.side} Qty={closing_order.qty}")
        return self.rest_session.place_order(
            category="linear", symbol=closing_order.symbol, side=closing_order.side,
            orderType="Market", qty=str(closing_order.qty),
            reduceOnly=True, orderLinkId=closing_order.client_order_id,
            # positionIdx=0
        )

    def _execute_close_order(self, close_req: Order):
        """Cria registro, envia, atualiza DB."""
        logger.info(f"Executando fechamento: CID={close_req.client_order_id} Side={close_req.side} Qty={close_req.qty}")
        closing_order_db = None
        with SessionLocal() as db_session:
            try:
                existing = db_session.query(Order).filter(Order.client_order_id == close_req.client_order_id).first()
                if existing:
                     logger.warning(f"Fechamento CID={close_req.client_order_id} já existe (Status: {existing.status}). Ignorando.")
                     return

                closing_order_db = Order(
                    client_order_id=close_req.client_order_id, symbol=close_req.symbol,
                    side=close_req.side, order_type="Market", qty=close_req.qty,
                    reduce_only=True, status='pending_submission'
                )
                db_session.add(closing_order_db)
                db_session.commit()
                # db_session.refresh(closing_order_db)

                response = self._send_close_order_to_api(closing_order_db)

                if response and response.get('retCode') == 0:
                    result = response.get('result', {})
                    oid_api = result.get('orderId')
                    order_to_update = db_session.query(Order).filter(Order.client_order_id == close_req.client_order_id).first()
                    if order_to_update:
                         order_to_update.order_id = oid_api
                         order_to_update.status = 'Submitted'
                         db_session.commit()
                         logger.info(f"Ordem fechamento CID={close_req.client_order_id} enviada V5. OID: {oid_api}")
                    else: logger.error(f"Ordem fechamento CID={close_req.client_order_id} sumiu do DB pós-envio!")
                else:
                    code = response.get('retCode', 'N/A')
                    msg = response.get('retMsg', 'Erro API')
                    logger.error(f"Erro API V5 (close): CID={close_req.client_order_id} Code={code} Msg='{msg}' | Resp: {response}")
                    order_to_update = db_session.query(Order).filter(Order.client_order_id == close_req.client_order_id).first()
                    if order_to_update:
                         order_to_update.status = 'failed'
                         db_session.commit()
                    self._publish_update(close_req.client_order_id, 'failed', error_msg=f"API Error Code={code}: {msg}")

            except (InvalidRequestError, FailedRequestError) as api_err:
                 code = getattr(api_err, 'ret_code', 'N/A')
                 msg = getattr(api_err, 'message', str(api_err))
                 logger.error(f"Erro API FINAL (close) CID={close_req.client_order_id}: Code={code} Msg='{msg}'", exc_info=False)
                 if db_session.is_active:
                     try:
                        order_to_update = db_session.query(Order).filter(Order.client_order_id == close_req.client_order_id).first()
                        if order_to_update and order_to_update.status == 'pending_submission':
                             order_to_update.status = 'failed'
                             db_session.commit()
                        else: db_session.rollback()
                     except Exception as db_err:
                          logger.error(f"Erro DB marcar falha API (close) CID={close_req.client_order_id}: {db_err}", exc_info=True)
                          db_session.rollback()
                 else: logger.error("Sessão DB inativa pós-erro API (close)")
                 self._publish_update(close_req.client_order_id, 'failed', error_msg=f"API Error Code={code}: {msg}")

            except Exception as e:
                logger.error(f"Erro GERAL (execute_close_order) CID={close_req.client_order_id}: {e}", exc_info=True)
                if db_session.is_active: db_session.rollback()
                self._publish_update(close_req.client_order_id or f"unknown_{uuid.uuid4()}", 'failed', error_msg=f"Internal Error: {e}")

    @retry(**RETRY_POLICY)
    def _send_modify_order_to_api(self, mod_request: Order):
        """Usa set_trading_stop da V5."""
        logger.debug(f"API V5 set_trading_stop: CID={mod_request.client_order_id} posIdx={mod_request.position_idx} SL={mod_request.new_stop_loss} TP={mod_request.new_take_profit}")
        sl_str = str(mod_request.new_stop_loss) if mod_request.new_stop_loss and mod_request.new_stop_loss > 0 else "0"
        tp_str = str(mod_request.new_take_profit) if mod_request.new_take_profit and mod_request.new_take_profit > 0 else "0"

        # Garantir que não estamos enviando None se 0.0 foi passado
        sl_str = sl_str if sl_str is not None else "0"
        tp_str = tp_str if tp_str is not None else "0"


        return self.rest_session.set_trading_stop(
            category="linear", symbol=mod_request.symbol,
            positionIdx=mod_request.position_idx,
            stopLoss=sl_str, takeProfit=tp_str,
            # tpslMode="Full" # Assumir Full mode
        )

    def _execute_modify_order(self, mod_request: Order):
        """Envia modificação para API e publica resultado no Redis."""
        logger.info(f"Executando modificação SL/TP: CID={mod_request.client_order_id} posIdx={mod_request.position_idx} NewSL={mod_request.new_stop_loss} NewTP={mod_request.new_take_profit}")
        try:
            response = self._send_modify_order_to_api(mod_request)

            if response and response.get('retCode') == 0:
                logger.info(f"Modificação SL/TP CID={mod_request.client_order_id} enviada com sucesso.")
                self._publish_update(mod_request.client_order_id, 'Modified')
            else:
                code = response.get('retCode', 'N/A')
                msg = response.get('retMsg', 'Erro API')
                if code == 110043: # SL/TP already the same or invalid condition
                     logger.warning(f"Falha Modificar SL/TP (Code=110043): SL/TP igual ou inválido. CID={mod_request.client_order_id}. Resp: {response}")
                     # Publicar falha para engine saber que não mudou
                     self._publish_update(mod_request.client_order_id, 'failed', error_msg=f"API Warning Code={code}: {msg}")
                else:
                     logger.error(f"Erro API V5 (set_trading_stop): CID={mod_request.client_order_id} Code={code} Msg='{msg}' | Resp: {response}")
                     self._publish_update(mod_request.client_order_id, 'failed', error_msg=f"API Error Code={code}: {msg}")

        except (InvalidRequestError, FailedRequestError) as api_err:
            code = getattr(api_err, 'ret_code', 'N/A')
            msg = getattr(api_err, 'message', str(api_err))
            logger.error(f"Erro API FINAL (set_trading_stop) CID={mod_request.client_order_id}: Code={code} Msg='{msg}'", exc_info=False)
            self._publish_update(mod_request.client_order_id, 'failed', error_msg=f"API Error Code={code}: {msg}")
        except Exception as e:
            logger.error(f"Erro GERAL (execute_modify_order) CID={mod_request.client_order_id}: {e}", exc_info=True)
            self._publish_update(mod_request.client_order_id, 'failed', error_msg=f"Internal Error: {e}")

    # --- Redis Handlers ---
    def _on_new_order_request(self, message):
        cid = f'error_{uuid.uuid4()}' # Default CID in case of json error
        try:
            data = json.loads(message['data'])
            cid = data.get('client_order_id', f'new_{uuid.uuid4()}')
            logger.info(f"Recebida requisição NOVA ORDEM: CID={cid}")
            order_req = Order(
                client_order_id=cid, symbol=data.get('symbol'), side=data.get('side'),
                order_type=data.get('order_type', 'Market'), qty=data.get('qty'),
                price=data.get('price'), stop_loss=data.get('stop_loss'),
                take_profit=data.get('take_profit'), status='received'
            )
            if not all([order_req.symbol, order_req.side, order_req.qty is not None and order_req.qty > 0]):
                 logger.error(f"Requisição nova ordem inválida (CID={cid}): {data}")
                 self._publish_update(cid, 'failed', error_msg="Dados inválidos (symbol/side/qty)")
            else:
                self._execute_new_order(order_req)
        except json.JSONDecodeError:
             logger.error(f"Erro JSON decode em _on_new_order_request: {message.get('data')}")
             self._publish_update(cid, 'failed', error_msg="Erro JSON na requisição")
        except Exception as e:
            logger.error(f"Erro CRÍTICO _on_new_order_request (CID={cid}): {e}", exc_info=True)
            self._publish_update(cid, 'failed', error_msg=f"Erro interno: {e}")
        finally:
            try: self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))
            except: pass # Ignore heartbeat errors here

    def _on_close_order_request(self, message):
        cid = f'error_{uuid.uuid4()}'
        try:
            data = json.loads(message['data'])
            cid = data.get('client_order_id', f'close_{uuid.uuid4()}')
            logger.info(f"Recebida requisição FECHAMENTO: CID={cid}")
            close_req = Order(
                client_order_id=cid, symbol=data.get('symbol'), side=data.get('side'),
                order_type="Market", qty=data.get('qty'), reduce_only=True, status='received'
            )
            if not all([close_req.symbol, close_req.side, close_req.qty is not None and close_req.qty > 0]):
                 logger.error(f"Requisição fechamento inválida (CID={cid}): {data}")
                 self._publish_update(cid, 'failed', error_msg="Dados inválidos (symbol/side/qty)")
            else:
                self._execute_close_order(close_req)
        except json.JSONDecodeError:
             logger.error(f"Erro JSON decode em _on_close_order_request: {message.get('data')}")
             self._publish_update(cid, 'failed', error_msg="Erro JSON na requisição")
        except Exception as e:
            logger.error(f"Erro CRÍTICO _on_close_order_request (CID={cid}): {e}", exc_info=True)
            self._publish_update(cid, 'failed', error_msg=f"Erro interno: {e}")
        finally:
            try: self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))
            except: pass

    def _on_modify_order_request(self, message):
        cid = f'error_{uuid.uuid4()}'
        try:
            data = json.loads(message['data'])
            cid = data.get('client_order_id', f'mod_{uuid.uuid4()}')
            pos_idx = data.get('position_idx', 0)
            logger.info(f"Recebida requisição MODIFICAÇÃO SL/TP: CID={cid}, posIdx={pos_idx}")
            mod_req = Order(
                client_order_id=cid, symbol=data.get('symbol'), status='received',
                position_idx=pos_idx, new_stop_loss=data.get('new_stop_loss'),
                new_take_profit=data.get('new_take_profit')
            )
            if not mod_req.symbol:
                 logger.error(f"Requisição modificação inválida (CID={cid}, sem símbolo): {data}")
                 self._publish_update(cid, 'failed', error_msg="Símbolo não especificado")
            else:
                self._execute_modify_order(mod_req)
        except json.JSONDecodeError:
             logger.error(f"Erro JSON decode em _on_modify_order_request: {message.get('data')}")
             self._publish_update(cid, 'failed', error_msg="Erro JSON na requisição")
        except Exception as e:
            logger.error(f"Erro CRÍTICO _on_modify_order_request (CID={cid}): {e}", exc_info=True)
            self._publish_update(cid, 'failed', error_msg=f"Erro interno: {e}")
        finally:
            try: self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))
            except: pass


    def run(self):
        """Loop principal: Conecta WS, inscreve tópicos, escuta Redis."""
        logger.info(f"Iniciando OrderManager WebSocket V5 para {self.symbol}...")
        pubsub = None # Initialize pubsub
        try:
            # V5 stream privada para ordens - usar o método de subscrição correto
            self.ws_session.order_stream(callback=self._handle_order_update)
            logger.info("Inscrito no stream privado WebSocket V5 'order'.")

            # Configurar e iniciar escuta do Redis PubSub
            pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(**{
                NEW_ORDER_CHANNEL: self._on_new_order_request,
                CLOSE_ORDER_CHANNEL: self._on_close_order_request,
                MODIFY_ORDER_CHANNEL: self._on_modify_order_request,
            })
            channels_list = list(pubsub.channels.keys()) if pubsub.channels else []
            logger.info(f"OrderManager ouvindo canais Redis: {channels_list}")
            self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))

            # Loop bloqueante para escutar o Redis
            for message in pubsub.listen():
                 # Handlers são chamados automaticamente via subscribe.
                 # Enviar heartbeat após processar uma mensagem (dentro dos handlers agora).
                 pass # Manter o loop rodando

        except redis.exceptions.ConnectionError as redis_err:
             logger.critical(f"OrderManager perdeu conexão com Redis: {redis_err}. Encerrando.", exc_info=False)
        except KeyboardInterrupt: # Permitir Ctrl+C
             logger.info("OrderManager recebendo sinal de interrupção...")
        except Exception as loop_err:
             logger.critical(f"Erro fatal no setup ou loop do OrderManager: {loop_err}. Encerrando.", exc_info=True)
             try: TelegramAlerter().send_message(f"🚨 ERRO CRÍTICO OrderManager: {loop_err}")
             except: pass
        finally:
            logger.info("OrderManager encerrando...")
            if self.ws_session:
                 try: self.ws_session.exit()
                 except Exception as ws_exit_err: logger.error(f"Erro ao fechar WS: {ws_exit_err}")
            if pubsub:
                 try: pubsub.close()
                 except Exception as ps_close_err: logger.error(f"Erro ao fechar PubSub: {ps_close_err}")