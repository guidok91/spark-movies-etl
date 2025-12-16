FROM python:3.12-slim

ENV TZ=UTC
ENV PATH="/root/.local/bin:$PATH"

RUN apt-get update && \
    apt-get install -y default-jre-headless make git curl zip && \
    rm -rf /var/lib/apt/lists/*

COPY . .

RUN make setup && \
    make package
