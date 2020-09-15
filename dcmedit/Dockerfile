FROM 10.32.42.225:5000/ubuntu:18.04
LABEL maintainer="Shuai Liang <sliang@indocresearch.org>"

RUN sed 's/main$/main universe/' -i /etc/apt/sources.list
RUN http_proxy="http://proxy.charite.de:8080/" apt-get update -y
RUN http_proxy="http://proxy.charite.de:8080/" apt-get install -y python3-pip python3-dev build-essential default-jre-headless
RUN http_proxy="http://proxy.charite.de:8080/" apt-get install locales
RUN http_proxy="http://proxy.charite.de:8080/" pip3 install --upgrade pip

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

COPY ./ /dcm
RUN pip install -r /dcm/scripts/requirements.txt --proxy="http://proxy.charite.de:8080/"
RUN chmod 555 -R /dcm

WORKDIR /dcm