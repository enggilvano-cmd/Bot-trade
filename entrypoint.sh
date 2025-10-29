#!/bin/sh

echo "Entrypoint iniciado com sucesso."
echo "Executando o comando principal (CMD): $@"

exec "$@"