FROM python:3.7

RUN echo "deb http://http.debian.net/debian jessie-backports main" | tee --append /etc/apt/sources.list.d/jessie-backports.list > /dev/null && \
    apt-get update -q -y && \
    apt-get --allow-unauthenticated install -t jessie-backports openjdk-8-jre-headless vim git make iputils-ping zip -q -y && \
    apt-get clean -q -y && \
    apt-get autoclean -q -y && \
    apt-get autoremove -q -y

ENV MOVIES_HOME=/home/movies
WORKDIR $MOVIES_HOME

COPY ./programs ./programs
COPY ./tests ./tests
COPY ["config.json", "entrypoint.sh", "makefile", "requirements.txt", "./"]

RUN make init

CMD sh ./entrypoint.sh
