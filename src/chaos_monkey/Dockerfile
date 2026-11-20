FROM python:3.12-alpine

WORKDIR /app

COPY chaos_monkey/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY chaos_monkey/main.py .
ENTRYPOINT ["python3", "main.py"]