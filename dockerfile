FROM ubuntu:latest

RUN apt-get update && apt-get install -y netcat-openbsd

COPY server-validator.sh /server-validator.sh

ENTRYPOINT ["sh", "/server-validator.sh"]
