FROM python:3

RUN apt-get update -q -y && \
    apt-get install apt-transport-https ca-certificates wget dirmngr gnupg software-properties-common vim git make iputils-ping zip -q -y && \
    wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - && \
    add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ && \
    apt update -q -y && \
    apt install adoptopenjdk-8-hotspot -q -y && \
    apt-get clean -q -y && \
    apt-get autoclean -q -y && \
    apt-get autoremove -q -y

ENV MOVIES_HOME=/home/movies
WORKDIR $MOVIES_HOME

#COPY movies_etl ./movies_etl
#COPY ./tests ./tests
#COPY data_lake ./data_lake
#COPY ["setup.py", "config.yaml", "entrypoint.sh", "makefile", "requirements.txt", "./"]

RUN make setup && make build

CMD sh ./entrypoint.sh
