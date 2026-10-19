FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ app/
COPY static/ static/
EXPOSE 8080 8081
CMD ["sh", "-c", "uvicorn app.proxy:proxy --host 0.0.0.0 --port 8081 --log-level warning & exec uvicorn app.main:app --host 0.0.0.0 --port 8080"]
