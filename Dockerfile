FROM python:3.12-slim

WORKDIR /app

# Dependencies
COPY requirements_cloud_runner.txt /app/requirements_cloud_runner.txt
RUN pip install --no-cache-dir -r /app/requirements_cloud_runner.txt

# App code
COPY pilot_runner_server.py /app/pilot_runner_server.py
COPY pilot_engine.py /app/pilot_engine.py

ENV PORT=10000
EXPOSE 10000

# Render / generic: bind to 0.0.0.0 and use gunicorn
CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:${PORT}", "pilot_runner_server:app"]


