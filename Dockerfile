# Usa uma imagem Python leve (3.12 para compatibilidade com pandas-ta)
FROM python:3.12-slim

# Define o diretório de trabalho
WORKDIR /app

# Instala dependências do sistema (necessárias para psycopg2, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copia e instala as dependências Python
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip

# Instala todas as dependências do requirements.txt em um único passo
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código do projeto para o container
COPY . .

# Define o comando padrão para rodar a aplicação.
# O entrypoint.sh não é mais necessário para sincronização de tempo.
CMD ["python", "-u", "main.py"]