FROM 10.32.42.225:5000/python:3.7-buster
LABEL maintainer="Samantha Zhang <szhang@indocresearch.org>"
USER root

# RUN sed 's/main$/main universe/' -i /etc/apt/sources.list 
RUN http_proxy="http://proxy.charite.de:8080/" apt-get update -y 
RUN http_proxy="http://proxy.charite.de:8080/" apt-get update -y && http_proxy="http://proxy.charite.de:8080/" apt-get install -y rsync python3-pip python3-dev build-essential \
    default-jre-headless locales
RUN pip3 install --upgrade pip --proxy=http://proxy.charite.de:8080
COPY ./ /datacopy
WORKDIR /datacopy
RUN chmod 555 -R /datacopy
RUN pip install --no-cache-dir -r requirements.txt --user --proxy=http://proxy.charite.de:8080
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN ls
