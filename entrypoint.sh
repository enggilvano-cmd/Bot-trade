#!/bin/sh
# entrypoint.sh

# Garante que o script pare se um comando falhar
set -e

echo "Sincronizando o tempo do container com um servidor NTP..."
# O pacote 'ntpsec-ntpdate' instala o comando 'ntpdate'
ntpdate -s pool.ntp.org

echo "Sincronização de tempo concluída. Iniciando a aplicação Python..."
exec python main.py