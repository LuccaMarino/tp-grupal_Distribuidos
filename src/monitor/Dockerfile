FROM python:3.12-alpine

WORKDIR /app

COPY monitor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY monitor/ .

ENTRYPOINT ["python3", "main.py"]