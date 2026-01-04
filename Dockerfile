FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY hl7_parser.py .
COPY hl7_kafka.py .
COPY hl7_cli_light.py .
COPY hl7_cli.py .

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "hl7_cli.py"]
CMD ["--help"]