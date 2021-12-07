FROM python:3.7-buster
LABEL maintainer="Samantha Zhang <szhang@indocresearch.org>"

RUN apt-get update -y && apt-get install -y rsync python3-pip python3-dev build-essential \
    default-jre-headless locales && rm -rf /var/lib/apt/lists/*
RUN pip3 install --upgrade pip
COPY ./ /datacopy
WORKDIR /datacopy
RUN chmod 555 -R /datacopy
RUN pip install --no-cache-dir -r requirements.txt --user
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN ls
