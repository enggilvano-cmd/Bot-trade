import pandas as pd
import pandas_ta as ta
from strategies.base_strategy import BaseStrategy

class EmaRsiStrategy(BaseStrategy):
    def __init__(self, params: dict):
        super().__init__(params)
        
        self.short_ema_period = self.params.get('short_ema', 9)
        self.long_ema_period = self.params.get('long_ema', 21)
        self.rsi_period = self.params.get('rsi_period', 14)
        
        self.regime_filter_period = self.params.get('regime_filter_period', 200) 
        
        self.adx_period = self.params.get('adx_period', 14)
        self.adx_threshold = self.params.get('adx_threshold', 0)

        self.atr_period = self.params.get('atr_period', 14)
        
        self.rsi_conviction_threshold = self.params.get('rsi_conviction_threshold', 60) 
        self.high_conviction_risk_mult = self.params.get('high_conviction_risk_mult', 1.0) 
        self.low_conviction_risk_mult = self.params.get('low_conviction_risk_mult', 0.5)  
        
        self.ema_short_col = f"EMA_{self.short_ema_period}"
        self.ema_long_col = f"EMA_{self.long_ema_period}"
        self.rsi_col = f"RSI_{self.rsi_period}"
        self.regime_col = f"EMA_{self.regime_filter_period}"
        self.atr_col = "ATR"
        self.adx_col = f"ADX_{self.adx_period}"


    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        df[self.ema_short_col] = ta.ema(df['close'], length=self.short_ema_period)
        df[self.ema_long_col] = ta.ema(df['close'], length=self.long_ema_period)
        df[self.rsi_col] = ta.rsi(df['close'], length=self.rsi_period)
        df[self.atr_col] = ta.atr(df['high'], df['low'], df['close'], length=self.atr_period)
        
        df[self.regime_col] = ta.ema(df['close'], length=self.regime_filter_period)
        
        adx_df = ta.adx(df['high'], df['low'], df['close'], length=self.adx_period)
        if adx_df is not None and not adx_df.empty and self.adx_col in adx_df.columns:
            df[self.adx_col] = adx_df[self.adx_col]
        else:
            df[self.adx_col] = pd.NA # Preenche com NaN se o cálculo falhar
        
        return df

    def generate_signal(self, df: pd.DataFrame) -> dict | None:
        
        if len(df) < 3:
            return None
            
        last_candle = df.iloc[-1]
        prev_candle = df.iloc[-2]

        required_cols = [
            self.ema_long_col, self.rsi_col, self.regime_col, 
            self.atr_col, self.adx_col
        ]
        if pd.isna(last_candle[required_cols]).any(): 
            return None
        
        if pd.isna(prev_candle[self.ema_long_col]):
            return None
        is_trending = last_candle[self.adx_col] > self.adx_threshold

        is_uptrend_regime = last_candle['close'] > last_candle[self.regime_col]
        is_downtrend_regime = last_candle['close'] < last_candle[self.regime_col]
        
        cross_up = (last_candle[self.ema_short_col] > last_candle[self.ema_long_col]) and \
                   (prev_candle[self.ema_short_col] <= prev_candle[self.ema_long_col])
                   
        rsi_filter_long = last_candle[self.rsi_col] > 50

        buy_signal = cross_up and rsi_filter_long and is_uptrend_regime and is_trending

        cross_down = (last_candle[self.ema_short_col] < last_candle[self.ema_long_col]) and \
                     (prev_candle[self.ema_short_col] >= prev_candle[self.ema_long_col])

        rsi_filter_short = last_candle[self.rsi_col] < 50
        
        sell_signal = cross_down and rsi_filter_short and is_downtrend_regime and is_trending


        risk_multiplier = self.low_conviction_risk_mult
        
        if buy_signal:
            if last_candle[self.rsi_col] > self.rsi_conviction_threshold:
                risk_multiplier = self.high_conviction_risk_mult
            
            return {
                "signal": "long",
                "sl_base_price": last_candle['low'],
                "atr_value": last_candle[self.atr_col],
                "risk_multiplier": risk_multiplier,
                "adx_value": last_candle[self.adx_col]
            }

        if sell_signal:
            if last_candle[self.rsi_col] < (100 - self.rsi_conviction_threshold):
                risk_multiplier = self.high_conviction_risk_mult
            
            return {
                "signal": "short",
                "sl_base_price": last_candle['high'],
                "atr_value": last_candle[self.atr_col],
                "risk_multiplier": risk_multiplier,
                "adx_value": last_candle[self.adx_col]
            }
            
        return None