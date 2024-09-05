#!/bin/bash
echo "Nombre del archivo de salida: $1"
echo "Cantidad de clientes: $2"

touch $1

sudo docker build -f ./Dockerfile.generador -t mi-generador .

sudo docker run \
    -e OUTPUT_FILE_NAME=$1 \
    -e AMOUNT_OF_CLIENTS=$2 \
    -v $(pwd)/$1:/$1 \
    mi-generador

exit 0