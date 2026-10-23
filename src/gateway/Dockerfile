FROM python:3-alpine
RUN pip3 install pika

COPY gateway .

ENTRYPOINT ["python3", "main.py"]