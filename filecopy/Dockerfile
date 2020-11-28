FROM 10.32.42.225:5000/python:3.7-buster
LABEL maintainer="Shuai Liang <sliang@indocresearch.org>"
USER root

# RUN sed 's/main$/main universe/' -i /etc/apt/sources.list 
RUN http_proxy="http://proxy.charite.de:8080/" apt-get update -y 
RUN http_proxy="http://proxy.charite.de:8080/" apt-get install rsync -y
RUN http_proxy="http://proxy.charite.de:8080/" apt-get install locales -y

COPY ./ /datacopy
WORKDIR /datacopy
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN ls
