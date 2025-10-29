#!/bin/sh
# entrypoint.sh

# Sincroniza o tempo do sistema do container usando um servidor NTP.

# É uma boa prática em aplicações financeiras para evitar problemas com timestamps.
echo "Sincronizando o tempo do container..."
if ! ntpdate -u pool.ntp.org; then
    echo "ERRO CRÍTICO: Falha ao sincronizar o tempo com ntpdate. Encerrando."
    exit 1
fi
echo "Sincronização de tempo concluída."

# O comando "$@" executa o que foi passado como CMD no Dockerfile
# ou na linha de comando (ex: docker run ... <comando>).
# Isso permite que o CMD seja facilmente sobrescrito, se necessário.
echo "Iniciando a aplicação principal..."
exec "$@"
