# -----------------------------------------------------------------
# Estágio 1: "Builder" - Usa a imagem completa para compilar dependências
# -----------------------------------------------------------------
FROM python:3.10 AS builder

# Instala dependências do sistema necessárias para COMPILAR pacotes Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Cria um ambiente virtual que será copiado para la imagem final
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Atualiza o pip
RUN pip install --upgrade pip

# Copia o arquivo de dependências
COPY requirements.txt .
# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# -----------------------------------------------------------------
# Estágio 2: "Final" - Usa a imagem slim para a execução
# -----------------------------------------------------------------
FROM python:3.10-slim

# Instala apenas as dependências de sistema MÍNIMAS para a EXECUÇÃO
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copia o ambiente virtual já pronto do estágio "builder"
COPY --from=builder /opt/venv /opt/venv

# Configura o PATH para usar o Python e os pacotes do nosso venv
ENV PATH="/opt/venv/bin:$PATH"

# Cria e define o diretório da aplicação
WORKDIR /app

# Copia o código da aplicação (incluindo o entrypoint.sh)
COPY . .
# [CORREÇÃO WINDOWS] Garante que o script seja executável E tenha as quebras de linha corretas
RUN sed -i 's/\r$//' /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Define o entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Altere "main.py" se o seu arquivo principal tiver outro nome
CMD ["python", "main.py"]