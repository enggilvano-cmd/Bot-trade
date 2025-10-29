import pandas as pd
import yaml

from backtesting import Backtest, Strategy as BacktestingStrategy
from strategies.ema_rsi_strategy import EmaRsiStrategy
from database.database import SessionLocal, engine
from database.models import Kline
from sqlalchemy import select

def load_data_from_db(symbol: str):
    """Carrega os dados históricos do banco de dados."""
    with SessionLocal() as db:
        query = select(Kline).where(Kline.symbol == symbol).order_by(Kline.timestamp)
        df = pd.read_sql(query, db.connection(), index_col='timestamp', parse_dates=['timestamp'])
        
        df.rename(columns={
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        }, inplace=True)
        
        return df

class StrategyBridge(BacktestingStrategy):
    
    config: dict = {}
    short_ema_period = 0
    long_ema_period = 0
    rsi_period = 0
    regime_filter_period = 0
    adx_period = 0
    adx_threshold = 0
    atr_period = 0
    atr_multiplier = 0.0
    risk_per_trade = 0.0
    tp1_risk_reward_ratio = 0.0
    tp1_close_percentage = 0.0
    move_sl_to_breakeven_on_tp1 = True
    rsi_conviction_threshold = 0
    high_conviction_risk_mult = 0.0
    low_conviction_risk_mult = 0.0
    
    def init(self):
        strat_params = self.config['strategy_params']
        risk_params = self.config['risk_params']
        
        self.short_ema_period = strat_params['short_ema']
        self.long_ema_period = strat_params['long_ema']
        self.rsi_period = strat_params['rsi_period']
        self.regime_filter_period = strat_params['regime_filter_period']
        self.adx_period = strat_params.get('adx_period', 14)
        self.adx_threshold = strat_params.get('adx_threshold', 0)
        
        self.atr_period = risk_params['atr_period']
        self.atr_multiplier = risk_params['atr_multiplier']
        self.risk_per_trade = risk_params['risk_per_trade']
        
        self.tp1_risk_reward_ratio = risk_params.get('tp1_risk_reward_ratio', 0.0)
        self.tp1_close_percentage = risk_params.get('tp1_close_percentage', 0.5)
        self.move_sl_to_breakeven_on_tp1 = risk_params.get('move_sl_to_breakeven_on_tp1', True)
        
        risk_conv_params = risk_params.get('dynamic_risk_params', {})
        self.rsi_conviction_threshold = risk_conv_params.get('rsi_conviction_threshold', 0)
        self.high_conviction_risk_mult = risk_conv_params.get('high_conviction_risk_mult', 1.0)
        self.low_conviction_risk_mult = risk_conv_params.get('low_conviction_risk_mult', 1.0)

        all_params = {**strat_params, **risk_params, **risk_conv_params, 'risk_reward_ratio': 0.0}
        self.strategy_obj = EmaRsiStrategy(all_params)

        df_ohlcv = pd.DataFrame({
            'open': self.data.Open, 'high': self.data.High, 'low': self.data.Low,
            'close': self.data.Close, 'volume': self.data.Volume
        })
        self.indicators_df = self.strategy_obj.calculate_indicators(df_ohlcv)
        self.ema_short = self.I(lambda: self.indicators_df[self.strategy_obj.ema_short_col], name="EMA Short")
        self.ema_long = self.I(lambda: self.indicators_df[self.strategy_obj.ema_long_col], name="EMA Long")
        self.rsi = self.I(lambda: self.indicators_df[self.strategy_obj.rsi_col], name="RSI", overlay=False)
        self.regime = self.I(lambda: self.indicators_df[self.strategy_obj.regime_col], name="Regime")
        self.atr = self.I(lambda: self.indicators_df[self.strategy_obj.atr_col], name="ATR", overlay=False)
        self.adx = self.I(lambda: self.indicators_df[self.strategy_obj.adx_col], name="ADX", overlay=False)

    def _handle_trailing_stop(self, current_df: pd.DataFrame):
        """Gerencia a lógica do Trailing Stop Loss (TSL)."""
        # Guarda de segurança: só executa se a posição estiver ativa e o TSL habilitado.
        if not self.position or self.atr_multiplier <= 0:
            return

        current_sl = self.position.sl
        # A guarda mais importante: se o SL não for um número, a posição foi fechada
        # pelo motor do backtester nesta vela. Não há nada a fazer.
        if not isinstance(current_sl, (int, float)):
            return

        atr_val = current_df[self.strategy_obj.atr_col].iloc[-1]
        atr_offset = atr_val * self.atr_multiplier

        if self.position.is_long:
            new_sl = self.data.Low[-1] - atr_offset
            if new_sl > current_sl:
                self.position.sl = new_sl
        elif self.position.is_short:
            new_sl = self.data.High[-1] + atr_offset
            if new_sl < current_sl:
                self.position.sl = new_sl

    def next(self):
        current_time = self.data.index[-1]
        price = self.data.Close[-1]

        try:
            current_df = self.indicators_df.loc[:current_time]
            if len(current_df) < 2: return 

            signal_data = self.strategy_obj.generate_signal(current_df)

            # --- GESTÃO DE POSIÇÃO E SAÍDAS ---
            if not self.position:
                # --- LÓGICA DE ENTRADA ---
                if signal_data:
                    sl_price = signal_data['sl_base_price']
                    risk_mult = signal_data.get('risk_multiplier', 1.0)
                    size = self._calculate_position_size(sl_price, risk_mult)

                    if size > 0:
                        if signal_data['signal'] == 'long':
                            self.buy(size=size, sl=sl_price)
                        elif signal_data['signal'] == 'short':
                            self.sell(size=size, sl=sl_price)
            else:
                # --- LÓGICA DE SAÍDA ---
                # 1. SAÍDA POR SINAL OPOSTO (PRIORIDADE MÁXIMA)
                is_long_and_short_signal = self.position.is_long and signal_data and signal_data['signal'] == 'short'
                is_short_and_long_signal = self.position.is_short and signal_data and signal_data['signal'] == 'long'
                if is_long_and_short_signal or is_short_and_long_signal:
                    self.position.close(comment="Sinal Oposto")
                    return # Sai para evitar outras ações na mesma vela

                # 2. GESTÃO DE TAKE PROFIT PARCIAL (TP1) E BREAKEVEN
                # Verifica se a posição tem apenas uma ordem de entrada (não é um remanescente de TP1)
                if len(self.trades) > 0 and self.trades[-1].is_entry and len(self.trades) % 2 != 0:
                    trade = self.trades[-1]
                    sl_dist = abs(trade.entry_price - trade.sl)
                    
                    if self.tp1_risk_reward_ratio > 0:
                        if self.position.is_long and self.data.High[-1] >= trade.entry_price + (sl_dist * self.tp1_risk_reward_ratio):
                            self.position.close(self.tp1_close_percentage, comment="TP1 Parcial Hit")
                            if self.move_sl_to_breakeven_on_tp1: self.position.sl = trade.entry_price
                        elif self.position.is_short and self.data.Low[-1] <= trade.entry_price - (sl_dist * self.tp1_risk_reward_ratio):
                            self.position.close(self.tp1_close_percentage, comment="TP1 Parcial Hit")
                            if self.move_sl_to_breakeven_on_tp1: self.position.sl = trade.entry_price
                
                # 3. GESTÃO DE TRAILING STOP (TSL)
                self._handle_trailing_stop(current_df)

        except Exception as e:
            print(f"Erro no Backtest 'next' em {current_time}: {e}")
            raise e

    def _calculate_position_size(self, stop_loss_price, risk_multiplier=1.0):
        risk_percent = self.risk_per_trade * risk_multiplier
        equity = self.equity
        entry_price = self.data.Close[-1]
        risk_per_trade_usd = equity * (risk_percent / 100)
        sl_distance_usd = abs(entry_price - stop_loss_price)
        if sl_distance_usd == 0: return 0.0
        size = risk_per_trade_usd / sl_distance_usd
        max_size = (equity * 0.95) / entry_price
        return min(size, max_size)


if __name__ == "__main__":
    
    with open('configs/btc_usdt_config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    print(f"Carregando dados para {config['symbol']} do banco de dados...")
    data = load_data_from_db(config['symbol'])
    
    if data.empty:
        print("Dados insuficientes. Execute o backfill.py primeiro.")
    else:
        print(f"Dados carregados: {len(data)} velas.")
        
        bt = Backtest(data, StrategyBridge, cash=10000, commission=.0006, trade_on_close=True) 

        print("Executando backtest...")
        stats = bt.run(config=config)
        print(stats)
        
        bt.plot()