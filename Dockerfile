FROM python:3.13.7-slim

WORKDIR /app

COPY . /app

RUN pip install -r requirements.txt

CMD ["uvicorn", "server:app", "--reload", "--port=8000", "--host=0.0.0.0"]
