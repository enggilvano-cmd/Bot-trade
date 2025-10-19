import os
import logging
import telegram
import asyncio

logger = logging.getLogger(__name__)

class TelegramAlerter:
    def __init__(self):
        # load_dotenv() # <-- REMOVIDO
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        if not self.bot_token or not self.chat_id:
            logger.warning("Credenciais do Telegram não encontradas. Alertas estão desativados.")
            self.bot = None
        else:
            try:
                self.bot = telegram.Bot(token=self.bot_token)
                logger.info("TelegramAlerter inicializado com sucesso.")
            except Exception as e:
                logger.error(f"Falha ao inicializar o bot do Telegram: {e}")
                self.bot = None

    def send_message(self, text: str):
        if not self.bot:
            # Não logar mensagens desativadas para não poluir o log
            # logger.info(f"Alerta (desativado): {text}")
            return

        try:
            # A biblioteca python-telegram-bot v20+ é assíncrona.
            # Usamos asyncio.run() para executar a corrotina de envio de mensagem.
            asyncio.run(self.bot.send_message(chat_id=self.chat_id, text=text))
        except Exception as e:
            logger.error(f"Falha ao enviar mensagem para o Telegram: {e}")