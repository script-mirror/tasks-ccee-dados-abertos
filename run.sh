#!/bin/bash

nome="$1"

echo "nome: ${nome}"

container_name="ccee-dados-abertos_$(date +%s)"

trap 'echo "Parando container..."; docker stop $container_name; docker rm $container_name; exit' SIGINT

docker run --rm --name $container_name \
  -v ~/.env:/root/.env \
  -e nome="$nome" \
  ccee-dados-abertos:latest