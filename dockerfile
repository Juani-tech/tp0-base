FROM python:3.12

ENV OUTPUT_FILE_NAME
ENV AMOUNT_OF_CLIENTS

COPY ./mi-generador.py .

RUN pip install pyyaml

CMD python3 mi-generador.py ${OUTPUT_FILE_NAME} ${AMOUNT_OF_CLIENTS}
