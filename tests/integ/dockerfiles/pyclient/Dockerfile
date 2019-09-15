FROM continuumio/miniconda3

COPY requirements.txt /
COPY tests /project
RUN pip install -r /requirements.txt

WORKDIR /project
ENV MARKETSTORE_HOST="localhost:5993"