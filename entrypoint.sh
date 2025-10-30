#!/bin/sh
# entrypoint.sh

# Garante que o script pare se um comando falhar
set -e

echo "Sincronizando o tempo do container com um servidor NTP..."
# O pacote 'ntpsec-ntpdate' instala o comando 'ntpdate'
# --- MELHORIA DE ROBUSTEZ ---
# Tenta o primeiro servidor; se falhar, tenta o segundo.
ntpdate -s pool.ntp.org || ntpdate -s time.google.com

echo "Sincronização de tempo concluída. Iniciando a aplicação Python..."
exec python main.py