FROM python:3.12-slim

WORKDIR /app

# Dependencies
COPY requirements_cloud_runner.txt /app/requirements_cloud_runner.txt
RUN pip install --no-cache-dir -r /app/requirements_cloud_runner.txt

# App code — v1 (kept for reference)
COPY pilot_runner_server.py /app/pilot_runner_server.py
COPY pilot_engine.py /app/pilot_engine.py

# App code — v2 modules
COPY pilot_runner_server_v2.py /app/pilot_runner_server_v2.py
COPY mapping_loader.py         /app/mapping_loader.py
COPY record_classifier.py      /app/record_classifier.py
COPY record_grouper.py         /app/record_grouper.py
COPY email_builder.py          /app/email_builder.py
COPY gmail_sender.py           /app/gmail_sender.py
COPY payload_builder.py        /app/payload_builder.py
COPY report_builder.py         /app/report_builder.py

ENV PORT=8080
EXPOSE 8080

CMD ["sh", "-c", "gunicorn -w 1 --timeout 3600 -b 0.0.0.0:${PORT} pilot_runner_server_v2:app"]


