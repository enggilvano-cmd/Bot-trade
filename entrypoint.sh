#!/bin/sh

# A Sincronização de tempo (ntpdate) foi removida.
# O container Docker usará automaticamente o relógio da sua máquina host (Windows 11).
# Tentar sincronizar o tempo de dentro do container era a causa do erro anterior.

echo "Entrypoint iniciado com sucesso."
echo "Executando o comando principal (CMD): $@"

# Este comando executa o que foi definido como CMD no seu Dockerfile ou docker-compose.yml
# (que no seu caso é "python main.py")
exec "$@"