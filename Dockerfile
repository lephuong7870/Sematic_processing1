FROM python:3.9-slim
RUN apt-get update 

WORKDIR /app
COPY data/. /data/.
COPY app/. /app/.
COPY requirements.txt /app/

RUN pip install -r /app/requirements.txt 
EXPOSE 8005
ENV PYTHONPATH=/app
CMD ["gunicorn", "-c", "gunicorn.conf.py", "main:app"]