FROM python:3.12-slim

WORKDIR /app
COPY ./devops/requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

COPY ./src /app/src

CMD [ "python", "src/main.py" ]