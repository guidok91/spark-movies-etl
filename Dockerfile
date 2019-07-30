FROM python:3.7

RUN apt-get update -q -y && \
    apt-get --allow-unauthenticated install openjdk-11-jre-headless vim git make iputils-ping zip -q -y && \
    apt-get clean -q -y && \
    apt-get autoclean -q -y && \
    apt-get autoremove -q -y

ENV MOVIES_HOME=/home/movies
WORKDIR $MOVIES_HOME

COPY ./programs ./programs
COPY ./tests ./tests
COPY ./movies_data_repository ./movies_data_repository
COPY ["config.yaml", "entrypoint.sh", "makefile", "requirements.txt", "./"]

RUN make init

CMD sh ./entrypoint.sh
