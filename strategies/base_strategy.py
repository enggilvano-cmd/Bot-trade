from abc import ABC, abstractmethod
import pandas as pd

class BaseStrategy(ABC):
    """Classe base abstrata para todas as estratégias."""
    
    def __init__(self, params: dict):
        self.params = params
            
    @abstractmethod
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcula os indicadores necessários para a estratégia."""
        pass
            
    @abstractmethod
    def generate_signal(self, df: pd.DataFrame) -> dict | None:
        """
        Gera um sinal ou None com base nos dados.
        Retorna um dicionário com os dados do sinal se houver oportunidade.
        Ex: {'signal': 'long', 'sl_base_price': 123.45, 'atr_value': 10.5, 'risk_multiplier': 1.0}
        """
        pass