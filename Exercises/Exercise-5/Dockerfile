FROM python:3.12.7

WORKDIR app
COPY ./data /app
COPY ./requirements.txt /app
COPY ./main.py /app

RUN python3 -m pip install -r requirements.txt