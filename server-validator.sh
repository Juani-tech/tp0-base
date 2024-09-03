#!/bin/bash

message="ping"

response=$(printf "$message" | nc -N server 12345 2>/dev/null)

# check if netcat executed successfully
# $? expands as: status code of the last command executed
if [ $? -ne 0 ]; then
  echo "action: test_echo_server | result: fail"
  exit 1
fi

if [ "$response" = "$message" ]; then
  echo "action: test_echo_server | result: success"
else
  echo "action: test_echo_server | result: fail"
fi
