FROM python:3.7-alpine
COPY . /app
WORKDIR /app
RUN pip install -r /app/ListenerProducer/requirements.txt
CMD ["/usr/local/bin/python Producer.py"]