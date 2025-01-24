FROM python:3.13-slim

ENV PATH="/root/.local/bin:$PATH"

RUN apt-get update && \
    apt-get install -y default-jre-headless make git curl zip && \
    rm -rf /var/lib/apt/lists/*

COPY . .

RUN make setup && \
    make package
