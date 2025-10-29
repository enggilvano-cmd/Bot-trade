import os
import time
import logging
import uuid
import json
import redis
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import desc, select, update
from database.database import SessionLocal
from database.models import Kline, Order
from strategies.ema_rsi_strategy import EmaRsiStrategy
from components.telegram_alerter import TelegramAlerter
from pybit.unified_trading import HTTP
from pybit.exceptions import InvalidRequestError, FailedRequestError 
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type


logger = logging.getLogger(__name__)
KLINE_CHANNEL = f"klines:{os.getenv('SYMBOL', 'BTCUSDT')}"
NEW_ORDER_CHANNEL = "orders:new"
MODIFY_ORDER_CHANNEL = "orders:modify"
ORDER_UPDATE_CHANNEL = "orders:update"

TE_RETRY_POLICY = {
    "wait": wait_exponential(multiplier=1, min=1, max=10),
    "stop": stop_after_attempt(3),
    "retry": retry_if_exception_type((InvalidRequestError, FailedRequestError)),
}

PRICE_PRECISION = 2 
QTY_PRECISION = 3   
MIN_ORDER_QTY = 0.001

PENDING_ORDER_TIMEOUT = 120

class TradingEngine:
    def __init__(self, config: dict, alerter: TelegramAlerter):
        self.config = config
        self.alerter = alerter

        all_params = {**config.get('strategy_params', {}), **config.get('risk_params', {})}
        strategy_name = config.get('strategy_name')
        if strategy_name == 'EmaRsiStrategy':
            try:
                self.strategy = EmaRsiStrategy(all_params)
                self.strategy_periods = [
                     self.strategy.short_ema_period, self.strategy.long_ema_period,
                     self.strategy.rsi_period, self.strategy.regime_filter_period,
                     self.strategy.adx_period, self.strategy.atr_period
                ]
            except (KeyError, ValueError, AttributeError) as e:
                 logger.critical(f"Erro ao inicializar estratégia '{strategy_name}': {e}", exc_info=True)
                 raise ValueError(f"Parâmetros inválidos para {strategy_name}: {e}")
        else:
             raise ValueError(f"Estratégia '{strategy_name}' não conhecida ou não definida.")

        self.symbol = config['symbol']
        try:
             self.warm_up_candles = int(config['engine_params']['warm_up_candles'])
             if self.warm_up_candles <= 0: raise ValueError("Deve ser > 0")
        except (KeyError, ValueError, TypeError):
             logger.warning("Parâmetro 'warm_up_candles' inválido/ausente. Usando default 500.")
             self.warm_up_candles = 500

        self.adx_threshold = float(config['strategy_params'].get('adx_threshold', 0))
        self.shadow_mode = config.get('shadow_mode', False)
        self.live_mode = config.get('live_mode', False)
        if self.shadow_mode and not self.live_mode:
             logger.warning("Modo Sombra ATIVO, mas live_mode=False. Desativando Modo Sombra.")
             self.shadow_mode = False

        risk_cfg = config.get('risk_params', {})
        self.tp1_rr = risk_cfg.get('tp1_risk_reward_ratio', 0.0)
        self.tp1_close_perc = risk_cfg.get('tp1_close_percentage', 0.5)
        self.move_sl_to_be = risk_cfg.get('move_sl_to_breakeven_on_tp1', True)
        self.max_neg_funding = risk_cfg.get('max_negative_funding_rate', -1.0)
        self.min_balance = risk_cfg.get('min_balance_usdt', 0)
        self.risk_per_trade = risk_cfg.get('risk_per_trade', 0)
        self.max_balance_risk_cap_per_trade = risk_cfg.get('max_balance_risk_cap_per_trade', 5.0) / 100

        self.pending_order = None
        self.active_order_cid = None
        self.active_position_details = {}

        self.df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
        self.df.index.name = 'timestamp'
        self.last_candle_time = None 

        try:
            self.redis_client = redis.Redis(
                host=os.getenv("REDIS_HOST", "localhost"), port=int(os.getenv("REDIS_PORT", 6379)),
                decode_responses=True, socket_connect_timeout=5, socket_timeout=5
            )
            self.redis_client.ping()
            logger.info("TradingEngine conectado ao Redis.")
        except redis.exceptions.ConnectionError as e:
            logger.critical(f"TradingEngine falhou ao conectar ao Redis: {e}")
            raise
        except Exception as e:
            logger.critical(f"TradingEngine falhou ao conectar ao Redis (Erro Geral): {e}", exc_info=True)
            raise

        try:
             self.rest_session = HTTP(
                 testnet=not self.live_mode,
                 api_key=os.getenv("BYBIT_API_KEY"),
                 api_secret=os.getenv("BYBIT_API_SECRET"),
                 recv_window=10000
             )
             time_res = self.rest_session.get_server_time()
             if not (time_res and time_res.get('retCode') == 0):
                  raise ConnectionError(f"Falha API V5 (get_server_time): {time_res}")
             logger.info(f"Conexão REST Bybit V5 estabelecida (Testnet: {not self.live_mode}).")
        except Exception as e:
             logger.critical(f"Falha CRÍTICA ao inicializar cliente REST Bybit V5: {e}", exc_info=True)
             raise

        self._load_and_warm_up_history()
        self._sync_position_on_startup()

        logger.info(f"TradingEngine V5 para {self.symbol} inicializado e pronto.")

    def _load_and_warm_up_history(self):
        logger.info(f"Aquecendo indicadores com até {self.warm_up_candles} velas...")
        try:
            with SessionLocal() as db_session:
                historical_candles = db_session.query(Kline).filter(
                    Kline.symbol == self.symbol
                ).order_by(Kline.timestamp.desc()).limit(self.warm_up_candles).all()
                historical_candles.reverse() 

            min_candles_needed = max(self.strategy_periods) + 50 if self.strategy_periods else 100

            if not historical_candles or len(historical_candles) < min_candles_needed:
                logger.warning(f"Dados históricos insuficientes ({len(historical_candles)}). Mínimo: {min_candles_needed}. Execute backfill.py.")
                self.df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
                self.df.index.name = 'timestamp'
                return

            data = [{'timestamp': k.timestamp, 'open': k.open, 'high': k.high, 'low': k.low, 'close': k.close, 'volume': k.volume} for k in historical_candles]
            temp_df = pd.DataFrame(data)
            temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'], utc=False).dt.tz_localize('UTC') 
            temp_df.set_index('timestamp', inplace=True)
            temp_df.sort_index(inplace=True)
            temp_df = temp_df[~temp_df.index.duplicated(keep='last')]
            self.df = temp_df
            self._calculate_indicators()
            
            if not self.df.empty:
                 self.last_candle_time = self.df.index[-1]
                 last_indicators = self.df.iloc[-1][[col for col in self.df.columns if col not in ['open','high','low','close','volume']]]
                 if last_indicators.isna().any():
                      nan_cols = last_indicators[last_indicators.isna()].index.tolist()
                      logger.warning(f"Warmup: Indicadores NaN: {nan_cols}.")
                 else:
                      logger.info(f"Warmup: Indicadores OK. Última vela: {self.last_candle_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            else:
                 logger.warning("Warmup: DataFrame vazio.")
        except Exception as e:
            logger.error(f"Erro CRÍTICO no warmup: {e}", exc_info=True)
            self.df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
            self.df.index.name = 'timestamp'

    def _calculate_indicators(self):
        if not self.df.empty:
            try:
                self.df = self.strategy.calculate_indicators(self.df.copy())
            except Exception as e:
                logger.error(f"Erro ao calcular indicadores: {e}", exc_info=True)

    def _sync_position_on_startup(self):
        try:
            logger.info("Sincronizando posição V5 e estado do DB...")
            pos_size, pos_side, entry_px, pos_idx, cur_sl, cur_tp = self._get_current_position()

            if pos_size > 0:
                order_cid_in_db = None
                try:
                    with SessionLocal() as db:
                        last_open_order = db.query(Order).filter(
                            Order.symbol == self.symbol,
                            Order.side == pos_side, # Lado da posição
                            Order.status == 'Filled',
                        ).order_by(Order.created_at.desc()).first()
                        
                        if last_open_order:
                            self.active_order_cid = last_open_order.client_order_id
                            self.active_position_details = {
                                'entry_price': last_open_order.entry_price or entry_px,
                                'tp1_price': last_open_order.tp1_price,
                                'is_tp1_hit': last_open_order.is_tp1_hit
                            }
                            order_cid_in_db = self.active_order_cid
                            logger.info(f"Estado sincronizado do DB (CID: {self.active_order_cid}): TP1 Atingido={last_open_order.is_tp1_hit}")
                        else:
                            logger.warning("Posição aberta encontrada, mas NENHUMA ordem de abertura 'Filled' no DB. Assumindo gerenciamento sem estado de TP1.")
                            self.active_position_details = {}
                            
                except Exception as e_db:
                     logger.error(f"Falha ao Sincronizar estado do DB: {e_db}", exc_info=True)

                entry_fmt = f"{entry_px:.{PRICE_PRECISION}f}"
                sl_fmt = f"{cur_sl:.{PRICE_PRECISION}f}" if cur_sl else "N/A"
                tp_fmt = f"{cur_tp:.{PRICE_PRECISION}f}" if cur_tp else "N/A"

                msg = (f"⚠️ POSIÇÃO V5 EXISTENTE!\nLado: {pos_side}, Qtd: {pos_size}\n"
                       f"Entrada: {entry_fmt}, SL: {sl_fmt}, TP: {tp_fmt}\n"
                       f"CID DB: {order_cid_in_db or 'N/A'}\nAssumindo gerenciamento.")
                logger.warning(msg)
                self.alerter.send_message(msg)
            else:
                logger.info("Nenhuma posição V5 aberta. Resetando estado.")
                self.active_order_cid = None
                self.active_position_details = {}

        except Exception as e:
             logger.error(f"Falha CRÍTICA sync posição V5: {e}", exc_info=True)
             self.alerter.send_message("🚨 Falha CRÍTICA sync posição V5!")

    @retry(**TE_RETRY_POLICY)
    def _get_wallet_balance(self, coin="USDT"):
        try:
            balance_info = self.rest_session.get_wallet_balance(accountType="CONTRACT")
            if balance_info and balance_info.get('retCode') == 0:
                 lst = balance_info.get('result', {}).get('list', [])
                 if lst:
                     for item in lst:
                         if item.get('coin') == coin:
                             bal = item.get('equity')
                             if bal:
                                 return float(bal)
            logger.error(f"Erro/formato saldo V5: {balance_info}")
            return 0.0
        except InvalidRequestError as api_err:
             logger.error(f"Erro API V5 (Saldo): {api_err.status_code}-{api_err.message}")
             self.alerter.send_message(f"🚨 Erro API Bybit (Saldo): {api_err.status_code}-{api_err.message}")
             return 0.0
        except Exception as e:
            logger.error(f"Erro geral saldo V5: {e}", exc_info=True)
            return 0.0

    @retry(**TE_RETRY_POLICY)
    def _get_live_price(self):
        try:
                ticker = self.rest_session.get_tickers(category="linear", symbol=self.symbol)
                if ticker and ticker.get('retCode') == 0:
                     lst = ticker.get('result', {}).get('list', [])
                     if lst:
                         price = lst[0].get('lastPrice')
                         if price:
                             return float(price)
                logger.warning(f"Erro/formato ticker V5: {ticker}")
                return None
        except InvalidRequestError as api_err:
             logger.error(f"Erro API V5 (Ticker): {api_err.status_code}-{api_err.message}")
             return None
        except Exception as e:
            logger.error(f"Erro geral preço V5: {e}. Usando 'close'.", exc_info=True)
            return None

    @retry(**TE_RETRY_POLICY)
    def _get_current_position(self):
        try:
                pos = self.rest_session.get_positions(category="linear", symbol=self.symbol)
                if pos and pos.get('retCode') == 0:
                    lst = pos.get('result', {}).get('list', [])
                    if lst:
                        for p in lst:
                            size = p.get('size', "0")
                            if size and float(size) > 0:
                                avg_px = p.get('avgPrice', "0");
                                sl = p.get('stopLoss', "0");
                                tp = p.get('takeProfit', "0")
                                return (float(size), p.get('side'),
                                        float(avg_px) if avg_px else 0.0,
                                        int(p.get('positionIdx', 0)),
                                        float(sl) if sl else 0.0,
                                        float(tp) if tp else 0.0)
                return 0.0, None, 0.0, 0, 0.0, 0.0
        except InvalidRequestError as api_err: # ... (rest of the file is truncated for brevity)
             logger.error(f"Erro API V5 (Posições): {api_err.status_code}-{api_err.message}")
             self.alerter.send_message(f"🚨 Erro API Bybit (Posições): {api_err.status_code}-{api_err.message}")
             return 0.0, None, 0.0, 0, 0.0, 0.0
        except Exception as e: # Catch all other exceptions
            logger.error(f"Erro geral posição V5: {e}", exc_info=True)
            return 0.0, None, 0.0, 0, 0.0, 0.0

    @retry(**TE_RETRY_POLICY)
    def _get_funding_rate(self):
        try:
                 ticker = self.rest_session.get_tickers(category="linear", symbol=self.symbol)
                 if ticker and ticker.get('retCode') == 0:
                     lst = ticker.get('result', {}).get('list', [])
                     if lst:
                         fr = lst[0].get('fundingRate')
                         if fr:
                             return float(fr)
                 logger.warning(f"Erro/formato funding V5: {ticker}")
                 return 0.0
        except Exception as e:
            logger.error(f"Erro geral funding V5: {e}", exc_info=True)
            return 0.0


    # --- Position Sizing ---
    def _calculate_position_size(self, entry_price, stop_loss_price, risk_multiplier: float = 1.0):
        balance = self._get_wallet_balance()
        
        if balance < self.min_balance:
            logger.warning(f"Saldo ({balance:.2f}) < Mínimo ({self.min_balance:.2f}).")
            return None

        if self.risk_per_trade <= 0: return None
        risk_percent = self.risk_per_trade / 100
        
        base_risk = balance * risk_percent
        risk_amount = min(base_risk * risk_multiplier, balance * self.max_balance_risk_cap_per_trade) # Limita o risco total
        
        logger.info(f"Calc Size: Saldo={balance:.2f}, RiscoFinal={risk_amount:.2f}")

        if entry_price <= 0 or stop_loss_price <= 0:
             logger.error(f"Preço de entrada ({entry_price}) ou SL ({stop_loss_price}) inválido.")
             return None
             
        sl_distance = abs(entry_price - stop_loss_price)
        if sl_distance == 0: return None

        qty = risk_amount / sl_distance
        
        if qty < MIN_ORDER_QTY:
             logger.warning(f"Qtd calculada ({qty:.{QTY_PRECISION+2}f}) < Mínima ({MIN_ORDER_QTY}).")
             return None

        return round(qty, QTY_PRECISION)


    # --- Redis Publishers ---
    def _publish_open_order(self, signal_data: dict, last_candle_close: float) -> str | None:
        """Calcula tamanho e publica a requisição de ABERTURA de ordem."""
        
        cid = f"bot_open_{uuid.uuid4().hex[:16]}"
        
        entry_price = self._get_live_price()
        # --- [CORREÇÃO CRÍTICA] Fail-safe de Risco ---
        if entry_price is None:
             logger.error(f"FALHA AO OBTER PREÇO AO VIVO. Ordem (CID={cid}) não será enviada.")
             self.alerter.send_message("🚨 ALERTA: Falha ao obter preço ao vivo. Ordem de abertura ignorada.")
             return None
        # --------------------------------------
        
        side = "Buy" if signal_data['signal'] == 'long' else "Sell"
        sl_base_price = signal_data['sl_base_price']
        risk_mult = signal_data.get('risk_multiplier', 1.0)
        
        stop_loss_price = round(sl_base_price, PRICE_PRECISION)

        # --- [NÍVEL AVANÇADO] Calcular TP1 (se R:R definido) ---
        tp1_price = 0.0
        if self.tp1_rr > 0:
            sl_distance = abs(entry_price - stop_loss_price)
            if side == "Buy":
                tp1_price = entry_price + (sl_distance * self.tp1_rr)
            else: # Sell
                tp1_price = entry_price - (sl_distance * self.tp1_rr)
            tp1_price = round(tp1_price, PRICE_PRECISION)
        # ----------------------------------------------------

        qty = self._calculate_position_size(entry_price, stop_loss_price, risk_mult)
        
        if qty is None or qty <= 0:
             logger.error(f"Tamanho da posição inválido ({qty}). Ordem CID={cid} não enviada.")
             return None

        # --- Filtro de Funding Rate (apenas para Long) ---
        if side == "Buy" and self.max_neg_funding < 0: 
             fund_rate = self._get_funding_rate()
             if fund_rate < self.max_neg_funding:
                 logger.warning(f"SINAL LONG IGNORADO. Funding ({fund_rate:.6f}) < limite ({self.max_neg_funding:.6f}). CID={cid}")
                 self.alerter.send_message(f"⚠️ Long Ignorado. Funding: {fund_rate*100:.4f}%")
                 return None

        # --- Publicar no Redis (ou logar se Shadow Mode) ---
        if self.shadow_mode:
             log_msg = (f"👻 MODO SOMBRA: ABRIR {side} {qty} @ {entry_price:.{PRICE_PRECISION}f} | "
                        f"SL={stop_loss_price:.{PRICE_PRECISION}f}, TP1={tp1_price:.{PRICE_PRECISION}f} | CID={cid}")
             logger.info(log_msg)
             self.alerter.send_message(log_msg)
             return cid 

        try:
            data = {
                "client_order_id": cid, "symbol": self.symbol, "side": side,
                "order_type": "Market", "qty": qty, "price": None,
                "stop_loss": None, # [CORREÇÃO DE EXECUÇÃO] Não enviar SL na abertura para evitar rejeições.
                
                # --- [NÍVEL AVANÇADO] Enviar dados de estado para o OM ---
                "take_profit": None, # Não enviar TP para a exchange
                "entry_price_estimate": entry_price, # Preço estimado
                "tp1_price": tp1_price if tp1_price > 0 else None,
                "tp1_rr": self.tp1_rr if self.tp1_rr > 0 else None
                # ---------------------------------------------------
            }
            # Armazena o SL que queremos definir após a confirmação do fill.
            data['sl_to_set_after_fill'] = stop_loss_price
            self.redis_client.publish(NEW_ORDER_CHANNEL, json.dumps(data))
            
            sl_fmt = f"{stop_loss_price:.{PRICE_PRECISION}f}"
            tp1_fmt = f"{tp1_price:.{PRICE_PRECISION}f}" if tp1_price > 0 else "N/A"
            risk_fmt = f"(Risco x{risk_mult:.1f})" if risk_mult != 1.0 else ""

            logger.info(f"Pub NEW {side} ({cid}): Qtd={qty} {risk_fmt} | SL={sl_fmt}, TP1={tp1_fmt}")
            self.alerter.send_message(f"🚀 NOVA ORDEM: {side} {qty} BTC\nSL: {sl_fmt}\nTP1: {tp1_fmt}\n{risk_fmt}")
            
            return cid
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Redis Pub Error (new): {e}")
        except Exception as e:
            logger.error(f"Erro Pub (new): {e}", exc_info=True)
        return None

    def _publish_modify_order(self, pos_idx: int, stop_loss: float, take_profit: float) -> str | None:
        """
        Publica uma requisição de MODIFICAÇÃO de SL/TP.
        'take_profit' = 0 significa CANCELAR o TP.
        """
        cid = f"bot_mod_{uuid.uuid4().hex[:16]}"
        sl = round(stop_loss, PRICE_PRECISION) if stop_loss is not None else None
        
        # O OM espera 0.0 para cancelar o TP
        tp = round(take_profit, PRICE_PRECISION) if take_profit is not None else None
        
        if self.shadow_mode:
             sl_fmt = f"{sl:.{PRICE_PRECISION}f}" if sl else "N/A"
             tp_fmt = f"{tp:.{PRICE_PRECISION}f}" if tp is not None else "N/A"
             log_msg = (f"👻 MODO SOMBRA: MODIFICAR (Idx={pos_idx}) | "
                        f"NewSL={sl_fmt}, NewTP={tp_fmt} | CID={cid}")
             logger.info(log_msg)
             return cid

        data = {"client_order_id": cid, "symbol": self.symbol, "position_idx": pos_idx,
                "new_stop_loss": sl, "new_take_profit": tp}
        try:
            self.redis_client.publish(MODIFY_ORDER_CHANNEL, json.dumps(data))
            logger.info(f"Pub MODIFY ({cid}) posIdx={pos_idx}: SL={sl}, TP={tp}")
            return cid
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Redis Pub Error (mod): {e}")
        except Exception as e:
            logger.error(f"Erro Pub (mod): {e}", exc_info=True)
        return None
        
    def _publish_close_order(self, qty: float, side_to_close: str, is_partial: bool = False) -> str | None:
        """Publica uma requisição de FECHAMENTO (Market) de ordem."""
        
        prefix = "bot_close_partial_" if is_partial else "bot_close_total_"
        cid = f"{prefix}{uuid.uuid4().hex[:16]}"
        
        # Ordem de fechamento é o lado oposto
        side = "Sell" if side_to_close == "Buy" else "Buy"

        if self.shadow_mode:
             log_msg = f"👻 MODO SOMBRA: FECHAR {side} {qty} (Market) | CID={cid}"
             logger.info(log_msg)
             return cid

        data = {"client_order_id": cid, "symbol": self.symbol, "side": side,
                "order_type": "Market", "qty": qty, "price": None,
                "stop_loss": None, "take_profit": None}
        try:
             self.redis_client.publish(NEW_ORDER_CHANNEL, json.dumps(data))
             logger.info(f"Pub CLOSE (via NEW) {side} ({cid}): Qtd={qty}")
             return cid
        except redis.exceptions.ConnectionError as e:
             logger.error(f"Redis Pub Error (close): {e}")
        except Exception as e:
             logger.error(f"Erro Pub (close): {e}", exc_info=True)
        return None
        
    def _update_order_state_in_db(self, cid: str, **kwargs):
        """Atualiza campos de estado (ex: is_tp1_hit) no DB."""
        if not cid:
            logger.error("Falha ao atualizar estado no DB: CID está nulo.")
            return
        
        logger.info(f"Atualizando estado do DB para {cid}: {kwargs}")
        try:
            with SessionLocal() as db:
                db.query(Order).filter(Order.client_order_id == cid).update(kwargs)
                db.commit()
        except Exception as e:
            logger.error(f"Erro CRÍTICO ao atualizar estado do DB para {cid}: {e}", exc_info=True)
            self.alerter.send_message(f"🚨 ERRO DB: Falha ao atualizar estado {cid}: {kwargs}")


    # --- Redis Listeners Handlers ---

    def _on_order_update(self, message):
        """
        Processa atualizações de status vindas do OrderManager.
        Esta é agora uma MÁQUINA DE ESTADOS para o TP1.
        """
        cid = "unknown"
        try:
            data = json.loads(message['data'])
            cid = data.get('client_order_id')
            
            if not self.pending_order or cid != self.pending_order["cid"]:
                # É uma atualização sobre uma ordem que não estamos esperando (ex: um TSL/TP hit)
                # O loop principal (on_new_candle) vai pegar a mudança de posição (pos_size=0)
                logger.debug(f"Recebida atualização não pendente: {cid} (Status: {data.get('status')})")
                return

            # É a atualização que estávamos esperando
            status = data.get('status')
            action = self.pending_order.get("action", "unknown")
            
            logger.info(f"Confirmação recebida: CID={cid}, Status={status}, Ação={action}")
            
            if status == 'Filled':
                if action == "open":
                    # Ação 1 (Abertura) concluída.
                    self.active_order_cid = cid
                    self.active_position_details = {
                        'entry_price': data.get('entry_price'),
                        'tp1_price': data.get('tp1_price'),
                        'is_tp1_hit': False # Começa como falso
                    }
                    logger.info(f"Posição aberta e estado sincronizado: EP={data.get('entry_price')}, TP1={data.get('tp1_price')}")
                    self.alerter.send_message(f"🎉 ORDEM {cid} EXECUTADA. Preço: {data.get('avg_price'):.{PRICE_PRECISION}f}")
                    self.pending_order = None # Desbloquear

                    # --- [CORREÇÃO DE EXECUÇÃO] Etapa 2: Definir SL após a confirmação do fill ---
                    pos_idx = data.get("pos_idx", 0)
                    sl_to_set = data.get("sl_to_set_after_fill")
                    if sl_to_set and pos_idx is not None:
                        logger.info(f"Fill de abertura confirmado. Definindo SL={sl_to_set} para posIdx={pos_idx}")
                        mod_cid = self._publish_modify_order(pos_idx, sl_to_set, 0.0)
                        if mod_cid:
                            # Trava o motor novamente, aguardando a confirmação da MODIFICAÇÃO do SL.
                            self.pending_order = {
                                "cid": mod_cid,
                                "timestamp": time.time(), # Reinicia o timer do deadlock
                                "action": "set_sl_after_open",
                            }
                        else:
                            # Se a publicação da modificação do SL falhar, é uma emergência.
                            logger.critical(f"FALHA CRÍTICA ao publicar MODIFICAÇÃO de SL para {cid}. POSIÇÃO DESPROTEGIDA!")
                            self.alerter.send_message(f"🚨 CRÍTICO: Falha ao definir SL para {cid}. POSIÇÃO SEM PROTEÇÃO! FECHE MANUALMENTE.")
                            self.pending_order = None # Desbloquear se a publicação falhar

                elif action == "tp1_partial_close":
                    # Ação 2 (Fechamento Parcial) concluída.
                    self.alerter.send_message(f"💰 TP1 ATINGIDO. {data.get('qty')} BTC fechados.")
                    
                    # Agora, executar Ação 3: Mover SL para Breakeven
                    # pos_idx agora vem do payload da ordem de fechamento
                    pos_idx = data.get("pos_idx")
                    if pos_idx is None:
                         logger.error(f"Não foi possível mover SL para BE para {self.active_order_cid}: pos_idx ausente.")
                         self.pending_order = None
                         return

                    entry_price = self.active_position_details.get("entry_price")
                    
                    if self.move_sl_to_be and entry_price:
                        logger.info("TP1 Fechado. Movendo SL para Breakeven...")
                        new_sl = entry_price
                        new_tp = 0.0 # Cancelar qualquer TP existente
                        mod_cid = self._publish_modify_order(pos_idx, new_sl, new_tp)
                        
                        if mod_cid:
                            # Trava o motor, aguardando a confirmação da modificação do SL para BE.
                            self.pending_order = {
                                "cid": mod_cid,
                                "timestamp": time.time(), # Reinicia o timer do deadlock
                                "original_open_cid": self.active_order_cid, # Referência
                                "action": "tp1_move_sl_be"
                            }
                            # Atualizar estado no DB e memória
                            self._update_order_state_in_db(self.active_order_cid, is_tp1_hit=True)
                            self.active_position_details['is_tp1_hit'] = True
                        else:
                            logger.error("Falha ao enviar Modificação (Mover SL BE). Desbloqueando.")
                            self.pending_order = None # Desbloquear
                    else:
                        logger.info("TP1 Fechado. SL não será movido (desativado ou EP nulo). Desbloqueando.")
                        self._update_order_state_in_db(self.active_order_cid, is_tp1_hit=True)
                        self.active_position_details['is_tp1_hit'] = True
                        self.pending_order = None # Desbloquear

                elif action == "close_total":
                    # Ação de fechamento total (sinal oposto) concluída.
                    self.alerter.send_message(f"✅ POSIÇÃO FECHADA (Total). {data.get('qty')} BTC.")
                    self.active_order_cid = None
                    self.active_position_details = {}
                    self.pending_order = None # Desbloquear
                    
            elif status == 'Modified':
                if action == "tp1_move_sl_be":
                    # Ação 3 (Mover SL) concluída.
                    logger.info("SL movido para Breakeven com sucesso. Máquina de estados TP1 concluída.")
                    self.alerter.send_message(f"🛡️ SL movido para Breakeven. Posição restante está sem risco.")
                    self.pending_order = None # Desbloquear
                elif action == "set_sl_after_open":
                    # Confirmação de que o SL inicial foi definido.
                    logger.info(f"SL inicial para {self.active_order_cid} definido com sucesso.")
                    self.alerter.send_message("✅ SL inicial definido. Posição protegida.")
                    self.pending_order = None # Desbloquear
                else:
                    logger.info(f"Modificação {cid} (Ação: {action}) confirmada. Desbloqueando.")
                    self.pending_order = None # Desbloquear
            
            elif status in ['Rejected', 'Cancelled', 'failed']:
                logger.error(f"Ordem {cid} (Ação: {action}) FALHOU/REJEITADA: {data.get('error')}")
                self.alerter.send_message(f"❌ Ordem {cid} (Ação: {action}) FALHOU: {data.get('error')}")
                self.pending_order = None # Desbloquear
                # Forçar ressincronização na próxima vela para corrigir o estado
                self.active_order_cid = None 

        except json.JSONDecodeError:
            logger.error(f"JSON Decode Error (Order Update): {message.get('data')}")
        except Exception as e:
            logger.error(f"Erro CRÍTICO _on_order_update (CID={cid}): {e}", exc_info=True)
        finally:
             try:
                 self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))
             except Exception as hb_e:
                 logger.error(f"TE Heartbeat Error (on_order_update): {hb_e}")

    def _on_new_candle(self, message):
        try:
            # --- [CORREÇÃO DE DEADLOCK] Lógica de verificação de Lock e Timeout ---
            if self.pending_order:
                cid = self.pending_order["cid"]
                age = time.time() - self.pending_order["timestamp"]
                
                if age > PENDING_ORDER_TIMEOUT:
                    logger.critical(f"DEADLOCK DETECTADO! Ordem {cid} pendente por {age:.0f}s.")
                    self.alerter.send_message(f"🚨 CRÍTICO: Deadlock detectado! Ordem {cid} sem resposta. Forçando ressync.")
                    self.pending_order = None # Forçar desbloqueio
                    self._sync_position_on_startup() # Ressincronizar estado com a exchange
                else:
                    return # Bloqueado, mas sem timeout ainda
                
            candle = json.loads(message['data'])
            
            try:
                candle_time = pd.Timestamp(candle['timestamp'], tz='UTC')
            except Exception as ts_err:
                 logger.error(f"Timestamp vela inválido: {candle.get('timestamp')}. Err: {ts_err}"); return

            if self.last_candle_time is not None and candle_time <= self.last_candle_time:
                return # Vela antiga/duplicada

            # Atualização do DataFrame
            new_data = {'open': float(candle['open']), 'high': float(candle['high']),
                        'low': float(candle['low']), 'close': float(candle['close']),
                        'volume': float(candle['volume'])}
            new_row = pd.DataFrame(new_data, index=[candle_time])
            if not isinstance(self.df.index, pd.DatetimeIndex):
                self.df.index = pd.to_datetime(self.df.index, utc=True)
            elif self.df.index.tz is None:
                 self.df.index = self.df.index.tz_localize('UTC')
            self.df = pd.concat([self.df, new_row])
            max_len = self.warm_up_candles + 200
            if len(self.df) > max_len:
                self.df = self.df.iloc[-max_len:]
            self.last_candle_time = candle_time
            self._calculate_indicators()
            last = self.df.iloc[-1]
            
            # Checagem de indicadores prontos
            req_inds = [self.strategy.atr_col, self.strategy.ema_short_col, self.strategy.ema_long_col,
                        self.strategy.rsi_col, self.strategy.regime_col]
            if self.adx_threshold > 0:
                 req_inds.append(self.strategy.adx_col)
            if pd.isna(last[req_inds]).any():
                return 

            # 1. Gerar Sinal
            signal_data = self.strategy.generate_signal(self.df)

            # 2. Obter Posição Atual
            pos_size, pos_side, entry_px, pos_idx, cur_sl, cur_tp = self._get_current_position()

            # 3. Lógica Principal: Delega para os handlers apropriados
            if pos_size > 0:
                self._handle_active_position(last, signal_data, pos_size, pos_side, entry_px, pos_idx, cur_sl, cur_tp)
            elif signal_data:
                self._handle_no_position(last, signal_data)

        except json.JSONDecodeError:
            logger.error(f"JSON Decode Error (Candle): {message.get('data')}")
        except Exception as e:
            logger.error(f"Erro CRÍTICO _on_new_candle: {e}", exc_info=True);
            self.alerter.send_message(f"🚨 ERRO CRÍTICO TE (vela): {e}")
        finally:
             try:
                 self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))
             except Exception as e:
                 logger.error(f"TE Heartbeat Error (on_candle): {e}")

    def _handle_active_position(self, last_candle: pd.Series, signal_data: dict, pos_size: float, pos_side: str, entry_px: float, pos_idx: int, cur_sl: float, cur_tp: float):
        """Lógica de gerenciamento para quando uma posição está ativa."""
        # Sincronizar estado se o TE reiniciou no meio de uma posição
        if not self.active_order_cid:
            logger.warning("Posição encontrada, mas estado interno vazio. Forçando ressync.")
            self._sync_position_on_startup()
            return # Tentar novamente na próxima vela

        # 1. Checar Sinal Oposto (Fechamento Total) - PRIORIDADE MÁXIMA
        if self._check_and_handle_close_signal(signal_data, pos_size, pos_side):
            return # Bloqueia outras ações se um fechamento foi iniciado

        # 2. Checar TP1 (Take Profit Parcial)
        if self._check_and_handle_tp1(last_candle, pos_size, pos_side, pos_idx):
            return # Bloqueia outras ações se o TP1 foi acionado

        # 3. Gerenciar Trailing Stop
        self._check_and_handle_trailing_stop(last_candle, pos_side, entry_px, pos_idx, cur_sl, cur_tp)

    def _check_and_handle_tp1(self, last_candle: pd.Series, pos_size: float, pos_side: str, pos_idx: int) -> bool:
        """Verifica se o TP1 foi atingido e inicia o processo de fechamento parcial."""
        details = self.active_position_details
        is_tp1_hit = details.get('is_tp1_hit', False)

        if not is_tp1_hit and self.tp1_rr > 0:
            tp1_price = details.get('tp1_price', 0.0)
            if tp1_price > 0:
                if (pos_side == 'Buy' and last_candle['high'] >= tp1_price) or \
                   (pos_side == 'Sell' and last_candle['low'] <= tp1_price):
                    
                    logger.info(f"*** TP1 ATINGIDO (Preço: {tp1_price}) ***")
                    close_qty = round(pos_size * self.tp1_close_perc, QTY_PRECISION)
                    
                    if close_qty < MIN_ORDER_QTY:
                        logger.warning(f"Qtd de fechamento parcial ({close_qty}) muito baixa. Ignorando TP1.")
                        self._update_order_state_in_db(self.active_order_cid, is_tp1_hit=True)
                        self.active_position_details['is_tp1_hit'] = True
                        return False # Não bloqueia, mas marca como feito
                    
                    cid_close = self._publish_close_order(close_qty, pos_side, is_partial=True)
                    if cid_close:
                        self.pending_order = {
                            "cid": cid_close, "timestamp": time.time(),
                            "action": "tp1_partial_close",
                            # Passar o pos_idx para a próxima etapa da máquina de estados
                            "pos_idx": pos_idx 
                        }
                    return True # Bloqueia
        return False

    def _check_and_handle_close_signal(self, signal_data: dict, pos_size: float, pos_side: str) -> bool:
        """Verifica se há um sinal oposto para fechar a posição inteira."""
        if signal_data:
            sig = signal_data['signal']
            if (pos_side == 'Buy' and sig == 'short') or (pos_side == 'Sell' and sig == 'long'):
                logger.info(f"Sinal OPOSTO ({sig}). Fechando {pos_side}...")
                self.alerter.send_message(f"🚨 SINAL OPOSTO: Fechando {pos_side}...")
                cid_close_total = self._publish_close_order(pos_size, pos_side, is_partial=False)
                if cid_close_total:
                    self.pending_order = {"cid": cid_close_total, "timestamp": time.time(), "action": "close_total"}
                return True # Bloqueia
        return False

    def _check_and_handle_trailing_stop(self, last_candle: pd.Series, pos_side: str, entry_px: float, pos_idx: int, cur_sl: float, cur_tp: float):
        """Calcula e aplica o Trailing Stop Loss."""
        atr_off = last_candle[self.strategy.atr_col] * self.config['risk_params'].get('atr_multiplier', 1.0)
        new_sl = None

        # Nota: O TP atual (cur_tp) será cancelado (setado para 0.0) se o trailing stop for ativado,
        # pois o bot assume o gerenciamento completo do TP1 e do SL.
        # Se houver um TP externo, ele será sobrescrito.
        
        if cur_sl is not None and cur_sl > 0:
            if pos_side == 'Buy':
                prop_sl = round(last_candle['low'] - atr_off, PRICE_PRECISION)
                if prop_sl > cur_sl: new_sl = prop_sl
            elif pos_side == 'Sell':
                prop_sl = round(last_candle['high'] + atr_off, PRICE_PRECISION)
                if prop_sl < cur_sl: new_sl = prop_sl
        
        if new_sl:
            if abs(new_sl - cur_sl) < (entry_px * 0.0001): return # Modificação insignificante
            
            logger.info(f"Trailing Stop: Movendo SL {pos_side} de {cur_sl:.{PRICE_PRECISION}f} -> {new_sl:.{PRICE_PRECISION}f}")
            self.alerter.send_message(f"📈 TRAILING STOP: SL -> {new_sl:.{PRICE_PRECISION}f}")

            # --- [CORREÇÃO DE BUG] Sempre cancelar o TP ao mover o Trailing Stop ---
            # O bot deve ter controle total da saída via TSL ou sinal oposto.
            # Enviar `cur_tp` poderia reativar um TP antigo que deveria ter sido cancelado
            # (ex: após o TP1). Enviar 0.0 garante que o TP na exchange seja removido.
            cid_mod = self._publish_modify_order(pos_idx, new_sl, 0.0)
            if cid_mod:
                self.pending_order = {"cid": cid_mod, "timestamp": time.time(), "action": "trailing_stop"}

    def _handle_no_position(self, last_candle: pd.Series, signal_data: dict):
        """Lógica de gerenciamento para quando não há posição e um sinal aparece."""
        adx = f"{signal_data.get('adx_value', 0):.1f}" if 'adx_value' in signal_data else "N/A"
        logger.info(f"SINAL ENTRADA: {signal_data['signal'].upper()} (ADX: {adx})")
        
        # Resetar estado (garantia)
        self.active_order_cid = None
        self.active_position_details = {}
        
        cid_open = self._publish_open_order(signal_data, last_candle['close'])
        if cid_open:
            self.pending_order = {
                "cid": cid_open,
                "timestamp": time.time(),
                "action": "open"
            }

    def run(self):
        # (Código inalterado, incluindo lógica de reconexão do PubSub)
        logger.info(f"Iniciando TradingEngine V5 para {self.symbol}...")
        pubsub = None
        try:
            pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(**{
                KLINE_CHANNEL: self._on_new_candle,
                ORDER_UPDATE_CHANNEL: self._on_order_update
            })
            logger.info(f"TE ouvindo Redis: {list(pubsub.channels.keys())}")
        except redis.exceptions.ConnectionError as e:
             logger.critical(f"TE falha subscribe Redis: {e}. Encerrando."); return
        except Exception as e:
             logger.critical(f"TE erro subscribe Redis: {e}. Encerrando.", exc_info=True); return
        
        try:
            self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))
        except: pass

        while True:
            try:
                # [CORREÇÃO DE ROBUSTEZ] Usar get_message com timeout para evitar bloqueio e permitir heartbeats.
                message = pubsub.get_message(timeout=1.0)
                if message:
                    pass
                else:
                    # Se não houver mensagens, envia um heartbeat para mostrar que o processo
                    # está vivo e não travado, mesmo sem atividade de trading.
                    self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))
            except KeyboardInterrupt:
                 logger.info("TradingEngine recebendo sinal de interrupção...")
                 break
            except Exception as e:
                 logger.critical(f"Erro FATAL no loop de mensagens do TE: {e}. Encerrando.", exc_info=True)
                 self.alerter.send_message(f"🚨 ERRO CRÍTICO TE (loop): {e}")
                 break

        logger.info("TradingEngine encerrando...")
        if pubsub:
            try: pubsub.close()
            except: pass