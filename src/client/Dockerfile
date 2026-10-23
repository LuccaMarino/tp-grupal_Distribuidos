FROM python:3-alpine
RUN pip3 install pika pyarrow

COPY common ./common
COPY client .

ENTRYPOINT ["python3", "main.py"]