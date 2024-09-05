FROM python:3.12


# default values here (overwritten if specified)
ENV OUTPUT_FILE_NAME=docker-compose-dev.yaml
ENV AMOUNT_OF_CLIENTS=1 

COPY ./mi-generador.py .

RUN pip install pyyaml

CMD python3 mi-generador.py ${OUTPUT_FILE_NAME} ${AMOUNT_OF_CLIENTS}
