FROM python:3.8.5-slim-buster

WORKDIR /app

COPY . /app

RUN pip install -r requirement.txt

CMD ["python3", "app.py"]

#python app.py
