import pandas as pd
import yaml
from backtesting import Backtest, Strategy as BacktestingStrategy
from strategies.ema_rsi_strategy import EmaRsiStrategy
from database.database import SessionLocal, engine
from database.models import Kline
from sqlalchemy import select

def load_data_from_db(symbol: str):
    """Carrega os dados históricos do banco de dados."""
    db = SessionLocal()
    query = select(Kline).where(Kline.symbol == symbol).order_by(Kline.timestamp)
    df = pd.read_sql(query, engine, index_col='timestamp', parse_dates=['timestamp'])
    
    df.rename(columns={
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'volume': 'Volume'
    }, inplace=True)
    
    db.close()
    return df

class StrategyBridge(BacktestingStrategy):
    
    # Parâmetros da Estratégia
    short_ema_period = 0
    long_ema_period = 0
    rsi_period = 0
    regime_filter_period = 0
    
    # --- [MELHORIA A++] Parâmetros ADX ---
    adx_period = 0
    adx_threshold = 0
    
    # Parâmetros de Risco
    atr_period = 0
    atr_multiplier = 0.0
    risk_per_trade = 0.0
    risk_reward_ratio = 0.0 # Usado apenas se > 0
    
    # Parâmetros de Risco Dinâmico
    rsi_conviction_threshold = 0
    high_conviction_risk_mult = 0.0
    low_conviction_risk_mult = 0.0
    
    def init(self):
        # Carrega os parâmetros do config
        strat_params = config['strategy_params']
        risk_params = config['risk_params']
        
        self.short_ema_period = self.I(lambda: strat_params['short_ema'])
        self.long_ema_period = self.I(lambda: strat_params['long_ema'])
        self.rsi_period = self.I(lambda: strat_params['rsi_period'])
        self.regime_filter_period = self.I(lambda: strat_params['regime_filter_period'])
        
        # --- [MELHORIA A++] Carrega ADX ---
        self.adx_period = self.I(lambda: strat_params.get('adx_period', 14))
        self.adx_threshold = self.I(lambda: strat_params.get('adx_threshold', 0))
        
        self.atr_period = self.I(lambda: risk_params['atr_period'])
        self.atr_multiplier = self.I(lambda: risk_params['atr_multiplier'])
        self.risk_per_trade = self.I(lambda: risk_params['risk_per_trade'])
        self.risk_reward_ratio = self.I(lambda: risk_params['risk_reward_ratio'])
        
        self.rsi_conviction_threshold = self.I(lambda: risk_params['rsi_conviction_threshold'])
        self.high_conviction_risk_mult = self.I(lambda: risk_params['high_conviction_risk_mult'])
        self.low_conviction_risk_mult = self.I(lambda: risk_params['low_conviction_risk_mult'])

        # Instancia nossa lógica de estratégia real
        all_params = {**strat_params, **risk_params}
        self.strategy = EmaRsiStrategy(all_params)
        
        self.df = pd.DataFrame({
            'open': self.data.Open,
            'high': self.data.High,
            'low': self.data.Low,
            'close': self.data.Close,
            'volume': self.data.Volume
        })
        
        # Calcula os indicadores
        self.df = self.strategy.calculate_indicators(self.df)
        
        # Mapeia os indicadores para plotagem
        self.ema_short = self.I(lambda x: self.df[self.strategy.ema_short_col].values, name="EMA_Short")
        self.ema_long = self.I(lambda x: self.df[self.strategy.ema_long_col].values, name="EMA_Long")
        self.rsi = self.I(lambda x: self.df[self.strategy.rsi_col].values, name="RSI")
        self.ema_regime = self.I(lambda x: self.df[self.strategy.regime_col].values, name="EMA_Regime")
        self.atr_val = self.I(lambda x: self.df[self.strategy.atr_col].values, name="ATR") 
        self.adx_val = self.I(lambda x: self.df.get(self.strategy.adx_col, float('nan')), name="ADX")


    def next(self):
        current_index = len(self.data.Close) - 1
        
        if current_index < 2:
            return
            
        # Pega o DataFrame até a vela atual
        df_slice = self.df.iloc[:current_index + 1]
        
        # 1. Gera o sinal
        signal_data = self.strategy.generate_signal(df_slice)
        
        # 2. Obtém a vela atual para trailing (índice -1)
        last_candle_low = self.data.Low[-1]
        last_candle_high = self.data.High[-1]
        current_atr = self.atr_val[-1] 
        
        if pd.isna(current_atr):
            return

        atr_offset = current_atr * self.atr_multiplier

        # --- LÓGICA DE GERENCIAMENTO DE POSIÇÃO ---
        
        # 3. Se Posição Aberta: Gerenciar Saída
        if self.position:
            
            # 3a. Saída por Sinal Oposto
            if signal_data:
                signal = signal_data['signal']
                if (self.position.is_long and signal == 'short') or \
                   (self.position.is_short and signal == 'long'):
                    
                    self.position.close()

            # 3b. Lógica de Trailing Stop (Se ainda estiver posicionado)
            if self.position:
                if self.position.is_long:
                    proposed_sl = last_candle_low - atr_offset
                    if proposed_sl > self.position.sl:
                        self.position.sl = proposed_sl
                
                elif self.position.is_short:
                    proposed_sl = last_candle_high + atr_offset
                    if proposed_sl < self.position.sl:
                        self.position.sl = proposed_sl

        # 4. Se Sem Posição: Gerenciar Entrada
        if not self.position and signal_data:
            signal = signal_data['signal']
            sl_base = signal_data['sl_base_price']
            risk_multiplier = signal_data['risk_multiplier']
            
            entry_price = self.data.Close[-1] 
            take_profit_price = 0.0

            # 4a. Calcular SL/TP
            if signal == 'long':
                stop_loss_price = sl_base - atr_offset
                risk_per_coin = entry_price - stop_loss_price
                if self.risk_reward_ratio > 0:
                    take_profit_price = entry_price + (risk_per_coin * self.risk_reward_ratio)

            elif signal == 'short':
                stop_loss_price = sl_base + atr_offset
                risk_per_coin = stop_loss_price - entry_price
                if self.risk_reward_ratio > 0:
                    take_profit_price = entry_price - (risk_per_coin * self.risk_reward_ratio)

            if risk_per_coin <= 0:
                return

            # 4b. Calcular Tamanho da Posição
            balance = self.equity
            base_risk_amount = balance * (self.risk_per_trade / 100)
            risk_amount_per_trade = base_risk_amount * risk_multiplier
            position_size = risk_amount_per_trade / risk_per_coin
            
            if position_size <= 0:
                return

            # 4c. Executar Ordem
            tp_param = take_profit_price if take_profit_price > 0 else None
            
            if signal == 'long':
                self.buy(sl=stop_loss_price, tp=tp_param, size=position_size)
            elif signal == 'short':
                self.sell(sl=stop_loss_price, tp=tp_param, size=position_size)

if __name__ == "__main__":
    print("Iniciando backtest...")
    
    with open('configs/btc_usdt_config.yaml', 'r') as f:
        config = yaml.safe_load(f) # O config agora é global para a StrategyBridge

    try:
        data = load_data_from_db(config['symbol'])
        min_candles = config['strategy_params'].get('regime_filter_period', 200) + 50
        if data.empty or len(data) < min_candles:
            print(f"Não foram encontrados dados suficientes ({len(data)}) para o símbolo {config['symbol']}.")
            print(f"Mínimo necessário: {min_candles}. Execute o script 'backfill.py'.")
            exit(1)
        print(f"Carregados {len(data)} registros do banco de dados.")
    except Exception as e:
        print(f"Erro ao carregar dados do banco: {e}")
        exit(1)

    # Executa o backtest
    bt = Backtest(data, StrategyBridge, cash=10000, commission=.0006) # 0.06% de taxa
    stats = bt.run()
    print("\n--- RESULTADOS DO BACKTEST (LÓGICA DE PRODUÇÃO) ---")
    print(stats)
    
    print("\n--- Trades ---")
    print(stats['_trades'])
    
    bt.plot()