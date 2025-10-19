# Usa uma imagem Python leve (3.12 para compatibilidade com pandas-ta)
FROM python:3.12-slim

# Define o diretório de trabalho
WORKDIR /app

# Instala dependências do sistema (necessárias para psycopg2, etc.)
RUN apt-get update && apt-get install -y \
    build-essential \
    ntpsec-ntpdate \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copia e instala as dependências Python
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip

# Garante que qualquer versão antiga/quebrada seja removida
# O '|| true' evita que o build falhe se o pybit não estiver instalado ainda
RUN pip uninstall -y pybit || true

# Força a reinstalação da versão mais recente do pybit
RUN pip install --no-cache-dir --force-reinstall pybit

# Instala o RESTO das dependências do requirements.txt
# (O pip é inteligente o suficiente para pular o pybit que já foi instalado)
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código do projeto para o container
COPY . .

# Copia e define o entrypoint para sincronizar o tempo
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]