FROM python:3-alpine
RUN pip3 install pika

COPY image_downloader/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY image_downloader .


ENTRYPOINT ["python3", "main.py"]