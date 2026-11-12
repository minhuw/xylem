FROM python:3.12-alpine
RUN pip install --no-cache-dir grpcio==1.85.1
COPY server.py /server.py
CMD ["python", "-u", "/server.py"]
