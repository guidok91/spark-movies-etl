FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y default-jre-headless make git && \
    rm -rf /var/lib/apt/lists/*

COPY . .

RUN make setup && \
    make package
