FROM python:3.8.5
COPY . /app

WORKDIR /app

RUN mkdir csv

RUN pip install -r requirements.txt

CMD ["python", "create_hyper.py"]
