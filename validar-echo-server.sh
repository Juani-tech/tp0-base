#!/bin/bash

message="ping"

server_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' server 2>/dev/null)

# -z: True if the length of string is zero.
if [ -z "$server_ip" ]; then
  echo "action: test_echo_server | result: fail"
  exit 1
fi

response=$(printf "$message" | nc -N "$server_ip" 12345 2>/dev/null)

# check if netcat executed successfully
# $? expands as: status code of the last command executed
if [ $? -ne 0 ]; then
  echo "action: test_echo_server | result: fail"
  exit 1
fi

if [ "$response" == "$message" ]; then
  echo "action: test_echo_server | result: success"
else
  echo "action: test_echo_server | result: fail"
fi
