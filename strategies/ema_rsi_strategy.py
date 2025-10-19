import pandas as pd
import pandas_ta as ta
from strategies.base_strategy import BaseStrategy

class EmaRsiStrategy(BaseStrategy):
    """
    [VERSÃO MELHORADA A++]
    Estratégia de cruzamento de MMEs com:
    1. Filtro de Regime (EMA 200)
    2. Filtro de Momentum (RSI)
    3. Filtro de Tendência (ADX)
    4. Risco Dinâmico (RSI Conviction)
    """

    def __init__(self, params: dict):
        super().__init__(params)
        
        # --- PARÂMETROS ESPERADOS (devem estar no config.yaml) ---
        
        # Parâmetros de Estratégia
        self.short_ema_period = self.params.get('short_ema', 9)
        self.long_ema_period = self.params.get('long_ema', 21)
        self.rsi_period = self.params.get('rsi_period', 14)
        
        # Filtro de Regime
        self.regime_filter_period = self.params.get('regime_filter_period', 200) 
        
        # --- [MELHORIA A++] Filtro ADX ---
        self.adx_period = self.params.get('adx_period', 14)
        self.adx_threshold = self.params.get('adx_threshold', 0) # 0 desativa o filtro

        # Parâmetros de Risco (usados para ATR)
        self.atr_period = self.params.get('atr_period', 14)
        
        # Risco Dinâmico
        self.rsi_conviction_threshold = self.params.get('rsi_conviction_threshold', 60) 
        self.high_conviction_risk_mult = self.params.get('high_conviction_risk_mult', 1.0) 
        self.low_conviction_risk_mult = self.params.get('low_conviction_risk_mult', 0.5)  
        
        # --- Nomes das colunas para fácil acesso ---
        self.ema_short_col = f"EMA_{self.short_ema_period}"
        self.ema_long_col = f"EMA_{self.long_ema_period}"
        self.rsi_col = f"RSI_{self.rsi_period}"
        self.regime_col = f"EMA_{self.regime_filter_period}"
        self.atr_col = "ATR"
        # --- [MELHORIA A++] Coluna ADX ---
        self.adx_col = f"ADX_{self.adx_period}"


    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy() # Evita SettingWithCopyWarning
        
        # Indicadores Padrão
        df[self.ema_short_col] = ta.ema(df['close'], length=self.short_ema_period)
        df[self.ema_long_col] = ta.ema(df['close'], length=self.long_ema_period)
        df[self.rsi_col] = ta.rsi(df['close'], length=self.rsi_period)
        df[self.atr_col] = ta.atr(df['high'], df['low'], df['close'], length=self.atr_period)
        
        # Indicador de Filtro de Regime
        df[self.regime_col] = ta.ema(df['close'], length=self.regime_filter_period)
        
        # --- [MELHORIA A++] Cálculo do ADX ---
        # pandas_ta.adx retorna ADX, DMP, DMN. Só precisamos do ADX (ex: 'ADX_14').
        adx_df = ta.adx(df['high'], df['low'], df['close'], length=self.adx_period)
        if adx_df is not None and not adx_df.empty and self.adx_col in adx_df.columns:
            df[self.adx_col] = adx_df[self.adx_col]
        else:
            df[self.adx_col] = pd.NA # Preenche com NaN se o cálculo falhar
        
        return df

    def generate_signal(self, df: pd.DataFrame) -> dict | None:
        
        # Garante que temos dados suficientes para olhar para trás
        if len(df) < 3:
            return None
            
        last_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]

        # Verifica se os indicadores já foram calculados (sem NaNs)
        required_cols = [
            self.ema_long_col, self.rsi_col, self.regime_col, 
            self.atr_col, self.adx_col # <-- ADX adicionado
        ]
        if pd.isna(last_candle[required_cols]).any(): 
            return None
        
        if pd.isna(prev_candle[self.ema_long_col]):
            return None

        # --- [MELHORIA A++] LÓGICA DO FILTRO ADX ---
        # Só opera se o mercado estiver em tendência (ADX > threshold)
        is_trending = last_candle[self.adx_col] > self.adx_threshold

        # --- LÓGICA DO FILTRO DE REGIME ---
        is_uptrend_regime = last_candle['close'] > last_candle[self.regime_col]
        is_downtrend_regime = last_candle['close'] < last_candle[self.regime_col]
        
        # --- Lógica de Sinais ---
        
        # Sinal de Compra (Long)
        cross_up = (last_candle[self.ema_short_col] > last_candle[self.ema_long_col]) and \
                   (prev_candle[self.ema_short_col] <= prev_candle[self.ema_long_col])
                   
        rsi_filter_long = last_candle[self.rsi_col] > 50

        # Condição final de Long (Cruzamento + Filtro RSI + Filtro de Regime + Filtro ADX)
        buy_signal = cross_up and rsi_filter_long and is_uptrend_regime and is_trending

        # Sinal de Venda (Short)
        cross_down = (last_candle[self.ema_short_col] < last_candle[self.ema_long_col]) and \
                     (prev_candle[self.ema_short_col] >= prev_candle[self.ema_long_col])

        rsi_filter_short = last_candle[self.rsi_col] < 50
        
        # Condição final de Short (Cruzamento + Filtro RSI + Filtro de Regime + Filtro ADX)
        sell_signal = cross_down and rsi_filter_short and is_downtrend_regime and is_trending


        # --- LÓGICA DE RISCO DINÂMICO ---
        risk_multiplier = self.low_conviction_risk_mult # Começa com o risco baixo
        
        if buy_signal:
            # Se o RSI for forte (ex: > 60), usa o multiplicador de risco alto
            if last_candle[self.rsi_col] > self.rsi_conviction_threshold:
                risk_multiplier = self.high_conviction_risk_mult
            
            return {
                "signal": "long",
                "sl_base_price": last_candle['low'],
                "atr_value": last_candle[self.atr_col],
                "risk_multiplier": risk_multiplier,
                "adx_value": last_candle[self.adx_col] # Para logging
            }

        if sell_signal:
            # Se o RSI for forte (ex: < 40), usa o multiplicador de risco alto
            # (40 é 100 - 60)
            if last_candle[self.rsi_col] < (100 - self.rsi_conviction_threshold):
                risk_multiplier = self.high_conviction_risk_mult
            
            return {
                "signal": "short",
                "sl_base_price": last_candle['high'],
                "atr_value": last_candle[self.atr_col],
                "risk_multiplier": risk_multiplier,
                "adx_value": last_candle[self.adx_col] # Para logging
            }
            
        return None