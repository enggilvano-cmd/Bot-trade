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
        # pd.read_sql can use the connection from the session or the global engine
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
    
    # Placeholder for config passed during bt.run()
    config: dict = {}

    # Parâmetros da Estratégia
    short_ema_period = 0
    long_ema_period = 0
    rsi_period = 0
    regime_filter_period = 0
    adx_period = 0
    adx_threshold = 0
    
    # Parâmetros de Risco
    atr_period = 0
    atr_multiplier = 0.0
    risk_per_trade = 0.0
    
    # --- [NÍVEL AVANÇADO] Parâmetros de TP Parcial ---
    tp1_risk_reward_ratio = 0.0
    tp1_close_percentage = 0.0
    move_sl_to_breakeven_on_tp1 = True
    
    # Parâmetros de Risco Dinâmico
    rsi_conviction_threshold = 0
    high_conviction_risk_mult = 0.0
    low_conviction_risk_mult = 0.0
    
    # --- [NÍVEL AVANÇADO] Estado interno para simular TP Parcial ---
    tp1_price = 0.0
    is_tp1_hit = False
    initial_sl_price = 0.0
    
    def init(self):
        # Carrega os parâmetros do config
        strat_params = self.config['strategy_params']
        risk_params = self.config['risk_params']
        
        self.short_ema_period = self.I(lambda: strat_params['short_ema'])
        self.long_ema_period = self.I(lambda: strat_params['long_ema'])
        self.rsi_period = self.I(lambda: strat_params['rsi_period'])
        self.regime_filter_period = self.I(lambda: strat_params['regime_filter_period'])
        self.adx_period = self.I(lambda: strat_params.get('adx_period', 14))
        self.adx_threshold = self.I(lambda: strat_params.get('adx_threshold', 0))
        
        self.atr_period = self.I(lambda: risk_params['atr_period'])
        self.atr_multiplier = self.I(lambda: risk_params['atr_multiplier'])
        self.risk_per_trade = self.I(lambda: risk_params['risk_per_trade'])
        
        # --- [NÍVEL AVANÇADO] Carregar configs de TP1 ---
        self.tp1_risk_reward_ratio = self.I(lambda: risk_params.get('tp1_risk_reward_ratio', 0.0))
        self.tp1_close_percentage = self.I(lambda: risk_params.get('tp1_close_percentage', 0.5))
        self.move_sl_to_breakeven_on_tp1 = self.I(lambda: risk_params.get('move_sl_to_breakeven_on_tp1', True))
        
        risk_conv_params = risk_params.get('dynamic_risk_params', {})
        self.rsi_conviction_threshold = self.I(lambda: risk_conv_params.get('rsi_conviction_threshold', 0)) 
        self.high_conviction_risk_mult = self.I(lambda: risk_conv_params.get('high_conviction_risk_mult', 1.0))
        self.low_conviction_risk_mult = self.I(lambda: risk_conv_params.get('low_conviction_risk_mult', 1.0))

        
        # Instancia a estratégia real
        # (O parâmetro 'risk_reward_ratio' não é mais usado pela estratégia, mas o mantemos aqui)
        all_params = {**strat_params, **risk_params, **risk_conv_params, 'risk_reward_ratio': 0.0}
        self.strategy_obj = EmaRsiStrategy(all_params)

        # (Carregamento de indicadores inalterado)
        df_ohlcv = pd.DataFrame({
            'open': self.data.Open, 'high': self.data.High, 'low': self.data.Low,
            'close': self.data.Close, 'volume': self.data.Volume
        })
        self.indicators_df = self.strategy_obj.calculate_indicators(df_ohlcv)
        self.ema_short = self.I(lambda df: self.indicators_df[self.strategy_obj.ema_short_col], name="EMA Short")
        self.ema_long = self.I(lambda df: self.indicators_df[self.strategy_obj.ema_long_col], name="EMA Long")
        self.rsi = self.I(lambda df: self.indicators_df[self.strategy_obj.rsi_col], name="RSI", overlay=False)
        self.regime = self.I(lambda df: self.indicators_df[self.strategy_obj.regime_col], name="Regime")
        self.atr = self.I(lambda df: self.indicators_df[self.strategy_obj.atr_col], name="ATR", overlay=False)
        self.adx = self.I(lambda df: self.indicators_df[self.strategy_obj.adx_col], name="ADX", overlay=False)


    def next(self):
        # Resetar estado se não houver posição
        if not self.position:
            self.is_tp1_hit = False
            self.tp1_price = 0.0
            self.initial_sl_price = 0.0
        
        current_time = self.data.index[-1]
        
        try:
            current_df = self.indicators_df.loc[:current_time]
            if len(current_df) < 2: return 

            signal_data = self.strategy_obj.generate_signal(current_df)

            # --- [NÍVEL AVANÇADO] Lógica de Simulação de TP Parcial ---
            # 1. Se em posição, checar se o TP1 foi atingido
            if self.position and not self.is_tp1_hit and self.tp1_risk_reward_ratio > 0:
                if self.position.is_long and self.data.High[-1] >= self.tp1_price:
                    # TP1 Atingido (Long)
                    self.is_tp1_hit = True
                    remaining_size = self.position.size * (1.0 - self.tp1_close_percentage)
                    
                    # Workaround: Fechar 100% da Posição...
                    self.position.close(price=self.tp1_price, comment="TP1 Parcial Hit")
                    
                    # ...e reabrir a posição restante com SL em Breakeven
                    if remaining_size > 0:
                        new_sl = self.position.entry_price if self.move_sl_to_breakeven_on_tp1 else self.initial_sl_price
                        self.buy(size=remaining_size, sl=new_sl, price=self.tp1_price, comment="TP1 Runner")
                    return # Ação da vela concluída
                    
                elif self.position.is_short and self.data.Low[-1] <= self.tp1_price:
                    # TP1 Atingido (Short)
                    self.is_tp1_hit = True
                    remaining_size = self.position.size * (1.0 - self.tp1_close_percentage)
                    
                    self.position.close(price=self.tp1_price, comment="TP1 Parcial Hit")
                    
                    if remaining_size > 0:
                        new_sl = self.position.entry_price if self.move_sl_to_breakeven_on_tp1 else self.initial_sl_price
                        self.sell(size=remaining_size, sl=new_sl, price=self.tp1_price, comment="TP1 Runner")
                    return # Ação da vela concluída

            # 2. Se não houver posição, checar sinal de entrada
            if not self.position:
                if signal_data and signal_data['signal'] == 'long':
                    sl_price = signal_data['sl_base_price']
                    risk_mult = signal_data.get('risk_multiplier', 1.0)
                    size = self._calculate_position_size(sl_price, risk_mult)
                    
                    if size and size > 0:
                        # Se TP1 estiver ativo, não definir TP na ordem
                        if self.tp1_risk_reward_ratio > 0:
                            self.initial_sl_price = sl_price
                            sl_dist = self.data.Close[-1] - sl_price
                            self.tp1_price = self.data.Close[-1] + (sl_dist * self.tp1_risk_reward_ratio)
                            self.buy(size=size, sl=sl_price)
                        else:
                            self.buy(size=size, sl=sl_price) # Operação normal
                            
                elif signal_data and signal_data['signal'] == 'short':
                    sl_price = signal_data['sl_base_price']
                    risk_mult = signal_data.get('risk_multiplier', 1.0)
                    size = self._calculate_position_size(sl_price, risk_mult)
                    
                    if size and size > 0:
                        if self.tp1_risk_reward_ratio > 0:
                            self.initial_sl_price = sl_price
                            sl_dist = sl_price - self.data.Close[-1]
                            self.tp1_price = self.data.Close[-1] - (sl_dist * self.tp1_risk_reward_ratio)
                            self.sell(size=size, sl=sl_price)
                        else:
                            self.sell(size=size, sl=sl_price)

            # 3. Se em posição, gerenciar sinal oposto e TSL
            elif self.position:
                # 3a. Checa sinal oposto
                if signal_data:
                    if self.position.is_long and signal_data['signal'] == 'short':
                        self.position.close(comment="Sinal Oposto")
                    elif self.position.is_short and signal_data['signal'] == 'long':
                        self.position.close(comment="Sinal Oposto")
                
                # 3b. Checa Trailing Stop
                current_sl = self.position.sl or 0
                atr_val = current_df[self.strategy_obj.atr_col].iloc[-1]
                atr_offset = atr_val * self.atr_multiplier
                
                if self.position.is_long:
                    new_sl = self.data.Low[-1] - atr_offset
                    if new_sl > current_sl: self.position.sl = new_sl
                elif self.position.is_short:
                    new_sl = self.data.High[-1] + atr_offset
                    if new_sl < current_sl: self.position.sl = new_sl
        
        except Exception as e:
            # Lidar com possíveis erros de índice se os dados não estiverem alinhados
            print(f"Erro no Backtest 'next' em {current_time}: {e}")
            pass


    def _calculate_position_size(self, stop_loss_price, risk_multiplier=1.0):
        # (Código inalterado)
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

    # 1. Carregar Dados
    print(f"Carregando dados para {config['symbol']} do banco de dados...")
    data = load_data_from_db(config['symbol'])
    
    if data.empty:
        print("Dados insuficientes. Execute o backfill.py primeiro.")
    else:
        print(f"Dados carregados: {len(data)} velas.")
        
        # 2. Configurar Backtest
        bt = Backtest(data, StrategyBridge, cash=10000, commission=.0006, trade_on_close=True) 

        # 3. Executar Backtest, passando o config para a estratégia
        print("Executando backtest...")
        stats = bt.run(config=config)
        print(stats)
        
        # 4. Plotar
        bt.plot()