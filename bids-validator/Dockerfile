FROM python:3.7-buster
LABEL maintainer="Samantha Zhang <szhang@indocresearch.org>"
USER root

# RUN sed 's/main$/main universe/' -i /etc/apt/sources.list 
RUN http_proxy="http://proxy.charite.de:8080/" apt-get update -y && http_proxy="http://proxy.charite.de:8080/" apt-get install -y rsync python3-pip python3-dev build-essential \
    default-jre-headless locales nodejs npm
RUN pip3 install --upgrade pip --proxy=http://proxy.charite.de:8080
RUN npm config set proxy "http://proxy.charite.de:8080/"
RUN npm config set https-proxy "http://proxy.charite.de:8080/"
RUN npm install -g bids-validator@1.8.0
COPY ./ /dataset-validate
WORKDIR /dataset-validate
RUN chmod 555 -R /dataset-validate
RUN pip install --no-cache-dir -r requirements.txt --user --proxy=http://proxy.charite.de:8080
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN ls

