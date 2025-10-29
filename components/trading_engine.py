import os
import time
import logging
import uuid
import json
import redis
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import desc, select
from database.database import SessionLocal
from database.models import Kline, Order
from strategies.ema_rsi_strategy import EmaRsiStrategy
from components.telegram_alerter import TelegramAlerter
from pybit.unified_trading import HTTP
from pybit.exceptions import InvalidRequestError, FailedRequestError

logger = logging.getLogger(__name__)
KLINE_CHANNEL = f"klines:{os.getenv('SYMBOL', 'BTCUSDT')}"
NEW_ORDER_CHANNEL = "orders:new"
MODIFY_ORDER_CHANNEL = "orders:modify"
CLOSE_ORDER_CHANNEL = "orders:close"
ORDER_UPDATE_CHANNEL = "orders:update"

# Constantes de precisão (ajustar conforme o par negociado, ex: BTCUSDT)
PRICE_PRECISION = 2 # Ex: 12345.67
QTY_PRECISION = 3   # Ex: 0.123
MIN_ORDER_QTY = 0.001

# --- [NOVO] Strategy Factory para modularidade ---
def strategy_factory(strategy_name: str, params: dict):
    """Carrega a classe da estratégia com base no nome."""
    strategies = {
        "EmaRsiStrategy": EmaRsiStrategy,
        # Adicione outras estratégias aqui
        # "OutraEstrategia": OutraEstrategia,
    }
    if strategy_name not in strategies:
        logger.critical(f"Estratégia '{strategy_name}' não conhecida.")
        raise ValueError(f"Estratégia '{strategy_name}' não conhecida.")
    
    try:
        return strategies[strategy_name](params)
    except (KeyError, ValueError, AttributeError) as e:
        logger.critical(f"Erro ao inicializar estratégia '{strategy_name}': {e}", exc_info=True)
        raise ValueError(f"Parâmetros inválidos para {strategy_name}: {e}")
# ------------------------------------------------

class TradingEngine:
    def __init__(self, config: dict, alerter: TelegramAlerter):
        self.config = config
        self.alerter = alerter

        # --- [CORREÇÃO] Carregamento dinâmico da estratégia ---
        all_params = {**config.get('strategy_params', {}), **config.get('risk_params', {})}
        strategy_name = config.get('strategy_name')
        if not strategy_name:
             raise ValueError("Nome da estratégia ('strategy_name') não definido.")
        
        self.strategy = strategy_factory(strategy_name, all_params)

        # Guardar períodos para validação de dados
        # (Tenta obter os períodos dinamicamente da estratégia carregada)
        self.strategy_periods = []
        attrs_to_check = ['short_ema_period', 'long_ema_period', 'rsi_period', 
                          'regime_filter_period', 'adx_period', 'atr_period']
        for attr in attrs_to_check:
            if hasattr(self.strategy, attr):
                self.strategy_periods.append(getattr(self.strategy, attr))
        # ------------------------------------------------------

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
        
        # --- [NOVO] Parâmetro para Stop-Limit ---
        self.slippage_percent = self.config.get('risk_params', {}).get('stop_limit_slippage_percent', 0.0)
        if self.slippage_percent > 0:
            logger.info(f"Usando ordens Stop-Limit com {self.slippage_percent*100:.2f}% de slippage permitido.")
        else:
            logger.info("Usando ordens Stop-Market (risco de slippage ilimitado).")
        # ----------------------------------------

        if self.shadow_mode and not self.live_mode:
             logger.warning("Modo Sombra ATIVO, mas live_mode=False. Desativando Modo Sombra.")
             self.shadow_mode = False

        self.pending_order_cid = None
        self.df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
        self.df.index.name = 'timestamp'
        self.last_candle_time = None # pd.Timestamp com timezone

        # Conexão Redis
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

        # Conexão Bybit REST API V5
        try:
             self.rest_session = HTTP(
                 testnet=not self.live_mode, # Usar testnet se NÃO live_mode
                 api_key=os.getenv("BYBIT_API_KEY"),
                 api_secret=os.getenv("BYBIT_API_SECRET"),
             )
             time_res = self.rest_session.get_server_time()
             if not (time_res and time_res.get('retCode') == 0):
                  raise ConnectionError(f"Falha API V5 (get_server_time): {time_res}")
             logger.info(f"Conexão REST Bybit V5 estabelecida (Testnet: {not self.live_mode}).")
        except Exception as e:
             logger.critical(f"Falha CRÍTICA ao inicializar cliente REST Bybit V5: {e}", exc_info=True)
             raise

        # Cargas iniciais
        self._load_and_warm_up_history()
        self._sync_position_on_startup()

        logger.info(f"TradingEngine V5 para {self.symbol} inicializado e pronto.")


    def _load_and_warm_up_history(self):
        """Carrega dados históricos do DB e calcula indicadores iniciais."""
        logger.info(f"Aquecendo indicadores com até {self.warm_up_candles} velas...")
        try:
            with SessionLocal() as db_session:
                # Corrigir: pegar as N mais recentes
                historical_candles = db_session.query(Kline).filter(
                    Kline.symbol == self.symbol
                ).order_by(Kline.timestamp.desc()).limit(self.warm_up_candles).all()
                historical_candles.reverse() # Ordenar do mais antigo para mais novo


            min_candles_needed = max(self.strategy_periods) + 50 if self.strategy_periods else 100

            if not historical_candles or len(historical_candles) < min_candles_needed:
                logger.warning(f"Dados históricos insuficientes ({len(historical_candles)}). Mínimo: {min_candles_needed}. Execute backfill.py.")
                self.df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
                self.df.index.name = 'timestamp'
                return

            data = [{'timestamp': k.timestamp, 'open': k.open, 'high': k.high, 'low': k.low, 'close': k.close, 'volume': k.volume} for k in historical_candles]
            temp_df = pd.DataFrame(data)

            # --- [CORREÇÃO] Robustez no carregamento de Timezone ---
            temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'])
            
            if temp_df['timestamp'].dt.tz is None:
                # Fallback caso o DB/SQLAlchemy retorne 'naive' (ex: SQLite)
                logger.warning("Timestamps do DB estão 'naive'. Localizando para UTC.")
                try:
                    temp_df['timestamp'] = temp_df['timestamp'].dt.tz_localize('UTC')
                except Exception as tz_err:
                    logger.error(f"Erro ao localizar timestamp 'naive' para UTC: {tz_err}. Pode haver dados mistos.")
                    # Tenta forçar, mas pode falhar se houver dados ambíguos
                    temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'], utc=True) 
            else:
                # Converte para UTC para garantir consistência (ex: se DB estiver em outro fuso)
                temp_df['timestamp'] = temp_df['timestamp'].dt.tz_convert('UTC')
            # --------------------------------------------------------

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
        """Calcula indicadores."""
        if not self.df.empty:
            try:
                self.df = self.strategy.calculate_indicators(self.df.copy())
            except Exception as e:
                logger.error(f"Erro ao calcular indicadores: {e}", exc_info=True)
        # else: logger.debug("DF vazio, skip indicadores.") # Log muito frequente


    # --- API Calls V5 (Usando pybit.v5.http) ---
    def _get_wallet_balance(self, coin="USDT"):
        # (Sem alterações)
        try:
            balance_info = self.rest_session.get_wallet_balance(accountType="CONTRACT")
            if balance_info and balance_info.get('retCode') == 0:
                 lst = balance_info.get('result', {}).get('list', [])
                 if lst:
                      for c in lst[0].get('coin', []):
                           if c['coin'] == coin: return float(c.get('availableToWithdraw', 0.0))
            logger.error(f"Erro/formato saldo V5: {balance_info}")
            return 0.0
        except InvalidRequestError as api_err:
             logger.error(f"Erro API V5 (Saldo): {api_err.status_code}-{api_err.message}")
             self.alerter.send_message(f"🚨 Erro API Bybit (Saldo): {api_err.status_code}-{api_err.message}")
             return 0.0
        except Exception as e:
            logger.error(f"Erro geral saldo V5: {e}", exc_info=True)
            return 0.0

    def _get_live_price(self):
        # (Sem alterações)
        try:
            ticker = self.rest_session.get_tickers(category="linear", symbol=self.symbol)
            if ticker and ticker.get('retCode') == 0:
                 lst = ticker.get('result', {}).get('list', [])
                 if lst:
                      price = lst[0].get('lastPrice')
                      if price: return float(price)
            logger.warning(f"Erro/formato ticker V5: {ticker}")
            return None
        except InvalidRequestError as api_err:
             logger.error(f"Erro API V5 (Ticker): {api_err.status_code}-{api_err.message}")
             return None # Não alertar, usar fallback
        except Exception as e:
            logger.error(f"Erro geral preço V5: {e}. Usando 'close'.", exc_info=True)
            return None

    def _get_current_position(self):
        # (Sem alterações)
        try:
            pos = self.rest_session.get_positions(category="linear", symbol=self.symbol)
            if pos and pos.get('retCode') == 0:
                 lst = pos.get('result', {}).get('list', [])
                 if lst:
                      for p in lst:
                           size = p.get('size', "0")
                           if size and float(size) > 0:
                                avg_px = p.get('avgPrice', "0"); sl = p.get('stopLoss', "0"); tp = p.get('takeProfit', "0")
                                return (float(size), p.get('side'), float(avg_px) if avg_px else 0.0,
                                        int(p.get('positionIdx', 0)), float(sl) if sl else 0.0, float(tp) if tp else 0.0)
            # logger.debug(f"Nenhuma posição V5 ou erro: {pos}")
            return 0.0, None, 0.0, 0, 0.0, 0.0
        except InvalidRequestError as api_err:
             logger.error(f"Erro API V5 (Posições): {api_err.status_code}-{api_err.message}")
             self.alerter.send_message(f"🚨 Erro API Bybit (Posições): {api_err.status_code}-{api_err.message}")
             return 0.0, None, 0.0, 0, 0.0, 0.0
        except Exception as e:
            if "Retryable error occurred" in str(e):
                logger.warning(f"Erro de API retentável ao buscar posições: {e}")
                return 0.0, None, 0.0, 0, 0.0, 0.0
            logger.error(f"Erro geral posição V5: {e}", exc_info=True)
            return 0.0, None, 0.0, 0, 0.0, 0.0

    def _get_funding_rate(self):
         # (Sem alterações)
         try:
            ticker = self.rest_session.get_tickers(category="linear", symbol=self.symbol)
            if ticker and ticker.get('retCode') == 0:
                 lst = ticker.get('result', {}).get('list', [])
                 if lst:
                      rate = lst[0].get('fundingRate')
                      if rate: return float(rate)
            logger.warning(f"Erro/formato funding V5 (ticker): {ticker}")
            return 0.0
         except InvalidRequestError as api_err:
              logger.error(f"Erro API V5 (Funding Rate): {api_err.status_code}-{api_err.message}")
              return 0.0
         except Exception as e:
            logger.error(f"Erro geral funding V5: {e}", exc_info=True)
            return 0.0

    # --- Startup Sync ---
    def _sync_position_on_startup(self):
        # (Sem alterações)
        logger.info("Sincronizando posição V5...")
        try:
            pos_size, pos_side, entry_px, pos_idx, cur_sl, cur_tp = self._get_current_position()
            if pos_size > 0:
                entry_fmt=f"{entry_px:.{PRICE_PRECISION}f}" if entry_px else "N/A"
                sl_fmt=f"{cur_sl:.{PRICE_PRECISION}f}" if cur_sl else "N/A"
                tp_fmt=f"{cur_tp:.{PRICE_PRECISION}f}" if cur_tp else "N/A"
                msg = (f"⚠️ POSIÇÃO V5 EXISTENTE!\nLado: {pos_side}, Qtd: {pos_size}\n"
                       f"Entrada: {entry_fmt}, SL: {sl_fmt}, TP: {tp_fmt}\nIdx: {pos_idx}\nAssumindo gerenciamento.")
                logger.warning(msg)
                self.alerter.send_message(msg)
            else: logger.info("Nenhuma posição V5 aberta.")
        except Exception as e:
            logger.error(f"Falha CRÍTICA sync posição V5: {e}", exc_info=True)
            self.alerter.send_message("🚨 Falha CRÍTICA sync posição V5!")

    # --- Position Sizing ---
    def _calculate_position_size(self, entry_price, stop_loss_price, risk_multiplier: float = 1.0):
        # (Sem alterações)
        balance = self._get_wallet_balance()
        min_balance = self.config['risk_params'].get('min_balance_usdt', 0)
        if balance < min_balance:
            logger.warning(f"Saldo ({balance:.2f}) < Mínimo ({min_balance:.2f}).")
            return None
        risk_percent = self.config['risk_params'].get('risk_per_trade', 0) / 100
        if risk_percent <= 0: return None
        base_risk = balance * risk_percent
        risk_amount = min(base_risk * risk_multiplier, balance * 0.95)
        logger.info(f"Calc Size: Saldo={balance:.2f}, RiscoFinal={risk_amount:.2f}")
        if entry_price <= 0 or stop_loss_price <= 0: return None
        risk_per_coin = abs(entry_price - stop_loss_price)
        if risk_per_coin < (10**-(PRICE_PRECISION + 2)): return None
        pos_size = risk_amount / risk_per_coin
        step_size = 10**-QTY_PRECISION
        pos_size = max(0, round((pos_size // step_size) * step_size, QTY_PRECISION))
        if pos_size < MIN_ORDER_QTY:
            logger.warning(f"Calc Size: Qtd ({pos_size}) < Mínimo ({MIN_ORDER_QTY}).")
            return None
        logger.info(f"Calc Size: Qtd final = {pos_size}")
        return pos_size

    # --- Redis Publishing ---
    def _publish_close_order(self, qty: float, side_to_close: str) -> str | None:
        # (Sem alterações)
        if self.shadow_mode:
            logger.info(f"[SHADOW] Ignorando fechamento {qty} {self.symbol} ({side_to_close})")
            cid = f"shadow_close_{int(time.time() * 1000)}"
            update = {"client_order_id": cid, "status": "Filled", "avg_price": 0.0}
            try: self.redis_client.publish(ORDER_UPDATE_CHANNEL, json.dumps(update))
            except Exception as e: logger.error(f"Erro pub fake update (close): {e}")
            return cid
        close_side = "Sell" if side_to_close == "Buy" else "Buy"
        cid = f"bot_close_{self.symbol}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:4]}"
        data = {"client_order_id": cid, "symbol": self.symbol, "side": close_side, "qty": qty}
        try:
            self.redis_client.publish(CLOSE_ORDER_CHANNEL, json.dumps(data))
            logger.info(f"Pub CLOSE ({cid}): {qty} {self.symbol} (Ordem: {close_side})")
            return cid
        except redis.exceptions.ConnectionError as e: logger.error(f"Redis Pub Error (close): {e}")
        except Exception as e: logger.error(f"Erro Pub (close): {e}", exc_info=True)
        return None

    def _publish_modify_order(self, pos_idx: int, new_sl: float, new_tp: float) -> str | None:
        if self.shadow_mode:
            logger.info(f"[SHADOW] Ignorando mod SL/TP (SL:{new_sl:.{PRICE_PRECISION}f}, TP:{new_tp:.{PRICE_PRECISION}f})")
            cid = f"shadow_mod_{int(time.time() * 1000)}"
            update = {"client_order_id": cid, "status": "Modified"}
            try: self.redis_client.publish(ORDER_UPDATE_CHANNEL, json.dumps(update))
            except Exception as e: logger.error(f"Erro pub fake update (mod): {e}")
            return cid
            
        cid = f"bot_mod_{self.symbol}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:4]}"
        sl = round(new_sl, PRICE_PRECISION) if new_sl and new_sl > 0 else 0.0
        tp = round(new_tp, PRICE_PRECISION) if new_tp and new_tp > 0 else 0.0
        
        # --- [NOVO] Calcular SL Limit Price para modificação (Trailing) ---
        sl_limit_px = 0.0
        if sl > 0 and self.slippage_percent > 0:
            # Precisamos saber o lado da posição para calcular o limite
            _pos_size, pos_side, _entry, _idx, _cur_sl, _cur_tp = self._get_current_position()
            if pos_side == "Buy":
                sl_limit_px = round(sl * (1 - self.slippage_percent), PRICE_PRECISION)
            elif pos_side == "Sell":
                sl_limit_px = round(sl * (1 + self.slippage_percent), PRICE_PRECISION)
            else:
                logger.warning(f"Não foi possível determinar o lado da posição para {cid}, SL-Limit não será usado.")
        # -----------------------------------------------------------------

        data = {
            "client_order_id": cid, "symbol": self.symbol, "position_idx": pos_idx, 
            "new_stop_loss": sl, 
            "new_take_profit": tp,
            "new_sl_limit_price": sl_limit_px # <-- NOVO
        }
        try:
            self.redis_client.publish(MODIFY_ORDER_CHANNEL, json.dumps(data))
            sl_type = f"SL-Limit={sl_limit_px:.{PRICE_PRECISION}f}" if sl_limit_px > 0 else "SL-Market"
            logger.info(f"Pub MODIFY ({cid}) posIdx={pos_idx}: {sl_type}, TP={tp:.{PRICE_PRECISION}f}")
            return cid
        except redis.exceptions.ConnectionError as e: logger.error(f"Redis Pub Error (mod): {e}")
        except Exception as e: logger.error(f"Erro Pub (mod): {e}", exc_info=True)
        return None

    def _publish_open_order(self, signal_data: dict, last_candle_close: float) -> str | None:
        signal=signal_data['signal']; sl_base=signal_data['sl_base_price']; atr_val=signal_data['atr_value']; risk_mult=signal_data.get('risk_multiplier', 1.0)
        max_neg_fund = self.config['risk_params'].get('max_negative_funding_rate', -1.0)
        if signal == 'long' and max_neg_fund < 0:
            fund_rate = self._get_funding_rate()
            if fund_rate < max_neg_fund:
                logger.warning(f"SINAL LONG IGNORADO. Funding ({fund_rate:.6f}) < limite ({max_neg_fund:.6f}).")
                self.alerter.send_message(f"⚠️ Long Ignorado. Funding: {fund_rate*100:.4f}%")
                return None
        
        entry_px = self._get_live_price()
        if entry_px is None: entry_px = last_candle_close; logger.warning("Usando close px para entrada.")
        atr_offset = atr_val * self.config['risk_params'].get('atr_multiplier', 1.0)
        rr = self.config['risk_params'].get('risk_reward_ratio', 0.0)
        
        sl_limit_px = 0.0 # Preço limite para Stop-Limit

        if signal == 'long':
            side = "Buy"; sl_px = round(sl_base - atr_offset, PRICE_PRECISION)
            risk = entry_px - sl_px; tp_px = round(entry_px + (risk * rr), PRICE_PRECISION) if rr > 0 else 0.0
            # --- [NOVO] Calcular SL Limit Price ---
            if sl_px > 0 and self.slippage_percent > 0:
                sl_limit_px = round(sl_px * (1 - self.slippage_percent), PRICE_PRECISION)
            # ------------------------------------
        else:
            side = "Sell"; sl_px = round(sl_base + atr_offset, PRICE_PRECISION)
            risk = sl_px - entry_px; tp_px = round(entry_px - (risk * rr), PRICE_PRECISION) if rr > 0 else 0.0
            # --- [NOVO] Calcular SL Limit Price ---
            if sl_px > 0 and self.slippage_percent > 0:
                sl_limit_px = round(sl_px * (1 + self.slippage_percent), PRICE_PRECISION)
            # ------------------------------------
            
        if risk <= (10**-(PRICE_PRECISION + 1)): logger.warning(f"Risco inválido (<=0). SL={sl_px}, Entry={entry_px}. Pulando."); return None
        qty = self._calculate_position_size(entry_px, sl_px, risk_mult)
        if not qty: logger.warning("Qtd inválida (None ou 0). Pulando."); return None
        
        cid = f"bot_open_{self.symbol}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:4]}"
        tp_txt = f"{tp_px:.{PRICE_PRECISION}f}" if tp_px > 0 else "N/A (Trailing)"
        adx_txt = f"{signal_data.get('adx_value', 0):.1f}" if 'adx_value' in signal_data else "N/A"
        
        sl_txt = f"{sl_px:.{PRICE_PRECISION}f}"
        if sl_limit_px > 0:
            sl_txt = f"{sl_px:.{PRICE_PRECISION}f} (Limit: {sl_limit_px:.{PRICE_PRECISION}f})"

        msg = (f"✅ REQ ORDEM ({cid})\nSinal: {signal.upper()} (Risco:{risk_mult*100:.1f}%, ADX:{adx_txt})\n"
               f"Qtd: {qty}\nSL: {sl_txt}\nTP: {tp_txt}")
               
        if self.shadow_mode:
            logger.info(f"[SHADOW] {msg}"); self.alerter.send_message(f"[SHADOW] {msg}")
            fake_cid = f"shadow_open_{int(time.time() * 1000)}"
            update = {"client_order_id": fake_cid, "status": "Filled", "avg_price": entry_px}
            try: self.redis_client.publish(ORDER_UPDATE_CHANNEL, json.dumps(update))
            except Exception as e: logger.error(f"Erro pub fake update (open): {e}")
            return fake_cid
            
        data = {
            "client_order_id": cid, "symbol": self.symbol, "side": side, "order_type": "Market", "qty": qty,
            "stop_loss": sl_px if sl_px > 0 else 0.0, 
            "sl_limit_price": sl_limit_px if sl_limit_px > 0 else 0.0, # <-- NOVO
            "take_profit": tp_px if tp_px > 0 else 0.0
        }
        
        try:
            self.redis_client.publish(NEW_ORDER_CHANNEL, json.dumps(data))
            logger.info(msg); self.alerter.send_message(msg)
            return cid
        except redis.exceptions.ConnectionError as e: logger.error(f"Redis Pub Error (open): {e}")
        except Exception as e: logger.error(f"Erro Pub (open): {e}", exc_info=True)
        return None

    # --- Event Handlers ---
    def _on_order_update(self, message):
        # (Sem alterações)
        cid = None # Garantir que cid está definido
        try:
            data = json.loads(message['data'])
            cid = data.get('client_order_id')

            if cid and cid == self.pending_order_cid:
                status = data.get('status')
                avg_price = data.get('avg_price', 'N/A')
                error_msg = data.get('error')
                logger.info(f"Confirmação recebida: CID={cid}, Status={status}. Desbloqueando.")
                self.pending_order_cid = None # Liberar lock

                if status == 'failed': self.alerter.send_message(f"🚨 FALHA ORDEM {cid}: {error_msg or '?'}")
                elif status == 'Filled':
                     px = f"{float(avg_price):.{PRICE_PRECISION}f}" if isinstance(avg_price, (float, int)) and avg_price != 0 else 'N/A'
                     self.alerter.send_message(f"🎉 ORDEM {cid} EXECUTADA. Preço: {px}")
                elif status == 'Modified': logger.info(f"Modificação {cid} confirmada.")
                elif status == 'Cancelled': logger.info(f"Ordem {cid} cancelada."); self.alerter.send_message(f"🚫 Ordem {cid} CANCELADA.")
                elif status == 'Rejected': logger.error(f"Ordem {cid} REJEITADA: {error_msg or '?'}") ; self.alerter.send_message(f"❌ Ordem {cid} REJEITADA: {error_msg or '?'}")

        except json.JSONDecodeError: logger.error(f"JSON Decode Error (Order Update): {message.get('data')}")
        except Exception as e: logger.error(f"Erro CRÍTICO _on_order_update (CID={cid}): {e}", exc_info=True)
        finally:
            try: self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))
            except Exception as hb_e: logger.error(f"TE Heartbeat Error (on_order_update): {hb_e}")

    def _on_new_candle(self, message):
         try:
            if self.pending_order_cid: return # Bloqueado

            candle = json.loads(message['data'])
            try: 
                # O timestamp do Redis já é ISO com fuso (UTC)
                candle_time = pd.Timestamp(candle['timestamp']) 
            except Exception as ts_err: 
                logger.error(f"Timestamp vela inválido: {candle.get('timestamp')}. Err: {ts_err}"); return

            if self.last_candle_time is not None and candle_time <= self.last_candle_time: return # Vela antiga/duplicada

            new_data = {'open': float(candle['open']), 'high': float(candle['high']), 'low': float(candle['low']),
                        'close': float(candle['close']), 'volume': float(candle['volume'])}
            new_row = pd.DataFrame(new_data, index=[candle_time])

            # --- [CORREÇÃO] Garantir que o DF tem fuso UTC ---
            # (O warmup agora garante isso, mas checagem dupla não faz mal)
            if not isinstance(self.df.index, pd.DatetimeIndex): 
                self.df.index = pd.to_datetime(self.df.index, utc=True)
            elif self.df.index.tz is None: 
                self.df.index = self.df.index.tz_localize('UTC')
            # ---------------------------------------------------

            self.df = pd.concat([self.df, new_row])
            max_len = self.warm_up_candles + 200
            if len(self.df) > max_len: self.df = self.df.iloc[-max_len:]
            self.last_candle_time = candle_time

            self._calculate_indicators()

            last = self.df.iloc[-1]
            # req_inds = [self.strategy.atr_col, self.strategy.ema_short_col, self.strategy.ema_long_col,
            #            self.strategy.rsi_col, self.strategy.regime_col]
            # --- [MELHORIA] Obter colunas da estratégia dinamicamente ---
            req_inds = [getattr(self.strategy, col) for col in ['atr_col', 'ema_short_col', 'ema_long_col', 'rsi_col', 'regime_col'] if hasattr(self.strategy, col)]
            if self.adx_threshold > 0 and hasattr(self.strategy, 'adx_col'): 
                req_inds.append(self.strategy.adx_col)
            # ----------------------------------------------------------
            
            if pd.isna(last[req_inds]).any(): return # Indicadores não prontos

            signal_data = self.strategy.generate_signal(self.df)
            pos_size, pos_side, entry_px, pos_idx, cur_sl, cur_tp = self._get_current_position()

            if pos_size > 0:
                if signal_data: # Checar sinal oposto
                    sig = signal_data['signal']
                    if (pos_side == 'Buy' and sig == 'short') or (pos_side == 'Sell' and sig == 'long'):
                        logger.info(f"Sinal OPOSTO ({sig}). Fechando {pos_side}...")
                        self.alerter.send_message(f"🚨 SINAL OPOSTO: Fechando {pos_side}...")
                        cid = self._publish_close_order(pos_size, pos_side)
                        if cid: self.pending_order_cid = cid
                        return

                # Trailing Stop
                atr_off = last[self.strategy.atr_col] * self.config['risk_params'].get('atr_multiplier', 1.0)
                new_sl = None
                if cur_sl is not None and cur_sl > 0:
                    if pos_side == 'Buy':
                        prop_sl = round(last['low'] - atr_off, PRICE_PRECISION)
                        if prop_sl > cur_sl: new_sl = prop_sl
                    elif pos_side == 'Sell':
                        prop_sl = round(last['high'] + atr_off, PRICE_PRECISION)
                        if prop_sl < cur_sl: new_sl = prop_sl
                # else: logger.debug("Trailing: SL atual não definido.")

                if new_sl:
                    logger.info(f"Trailing Stop: Movendo SL {pos_side} de {cur_sl:.{PRICE_PRECISION}f} -> {new_sl:.{PRICE_PRECISION}f}")
                    self.alerter.send_message(f"📈 TRAILING STOP: SL -> {new_sl:.{PRICE_PRECISION}f}")
                    cid = self._publish_modify_order(pos_idx, new_sl, cur_tp if cur_tp else 0.0)
                    if cid: self.pending_order_cid = cid
                    return

            elif signal_data: # Sem posição, checar entrada
                adx = f"{signal_data.get('adx_value', 0):.1f}" if 'adx_value' in signal_data else "N/A"
                logger.info(f"SINAL ENTRADA: {signal_data['signal'].upper()} (ADX: {adx})")
                cid = self._publish_open_order(signal_data, last['close'])
                if cid: self.pending_order_cid = cid
                return

         except json.JSONDecodeError: logger.error(f"JSON Decode Error (Candle): {message.get('data')}")
         except Exception as e: logger.error(f"Erro CRÍTICO _on_new_candle: {e}", exc_info=True); self.alerter.send_message(f"🚨 ERRO CRÍTICO TE (vela): {e}")
         finally:
            try: self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))
            except Exception as e: logger.error(f"TE Heartbeat Error (on_candle): {e}")


    def run(self):
        # (Sem alterações)
        logger.info(f"Iniciando TradingEngine V5 para {self.symbol}...")
        pubsub = None
        try:
             pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)
             pubsub.subscribe(**{ KLINE_CHANNEL: self._on_new_candle, ORDER_UPDATE_CHANNEL: self._on_order_update })
             logger.info(f"TE ouvindo Redis: {list(pubsub.channels.keys())}")
        except redis.exceptions.ConnectionError as e: logger.critical(f"TE falha subscribe Redis: {e}. Encerrando."); return
        except Exception as e: logger.critical(f"TE erro subscribe Redis: {e}. Encerrando.", exc_info=True); return

        try: self.redis_client.set(f"heartbeat:{self.__class__.__name__}", int(time.time()))
        except: pass # Ignorar falha no hb inicial

        while True:
            try:
                # Loop bloqueante que chama handlers
                for message in pubsub.listen(): pass
                logger.critical("Loop pubsub.listen() do TradingEngine terminou inesperadamente.")
                break
            except redis.exceptions.ConnectionError as e:
                 logger.critical(f"TE perdeu conexão Redis: {e}. Tentando reconectar pubsub...", exc_info=False)
                 try:
                      if pubsub: pubsub.close() # Fechar antigo
                 except: pass
                 while True:
                      time.sleep(10) # Esperar 10s
                      try:
                           logger.info("Tentando reconectar PubSub Redis...")
                           pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)
                           pubsub.subscribe(**{ KLINE_CHANNEL: self._on_new_candle, ORDER_UPDATE_CHANNEL: self._on_order_update })
                           logger.info("PubSub Redis reconectado!")
                           break # Sair do loop de reconexão
                      except redis.exceptions.ConnectionError:
                           logger.error("Falha ao reconectar PubSub Redis. Tentando novamente em 10s...")
                      except Exception as reconn_err:
                           logger.critical(f"Erro CRÍTICO ao tentar reconectar PubSub Redis: {reconn_err}. Encerrando TE.", exc_info=True)
                           self.alerter.send_message("🚨 ERRO CRÍTICO: TE não conseguiu reconectar ao Redis PubSub.")
                           return # Encerrar o processo run()
            except KeyboardInterrupt: # Permitir Ctrl+C
                 logger.info("TradingEngine recebendo sinal de interrupção...")
                 break # Sair do loop while
            except Exception as e:
                 logger.critical(f"Erro FATAL no loop listen() do TE: {e}. Encerrando.", exc_info=True)
                 self.alerter.send_message(f"🚨 ERRO CRÍTICO TE (loop): {e}")
                 break # Sair do loop while

        logger.info("TradingEngine encerrando...")
        if pubsub:
             try: pubsub.close()
             except: pass # Ignorar erro ao fechar