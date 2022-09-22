# Copyright 2022 Indoc Research
# 
# Licensed under the EUPL, Version 1.2 or – as soon they
# will be approved by the European Commission - subsequent
# versions of the EUPL (the "Licence");
# You may not use this work except in compliance with the
# Licence.
# You may obtain a copy of the Licence at:
# 
# https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
# 
# Unless required by applicable law or agreed to in
# writing, software distributed under the Licence is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
# See the Licence for the specific language governing
# permissions and limitations under the Licence.
# 

FROM python:3.7-buster
LABEL maintainer="Samantha Zhang <szhang@indocresearch.org>"

ARG pip_username
ARG pip_password

RUN apt-get update -y && apt-get install -y rsync python3-pip python3-dev build-essential \
    default-jre-headless locales nodejs npm && rm -rf /var/lib/apt/lists/*
RUN pip3 install --upgrade pip
RUN npm install -g bids-validator@1.8.0

COPY ./ /dataset-validate
WORKDIR /dataset-validate
RUN chmod 555 -R /dataset-validate
RUN PIP_USERNAME=$pip_username PIP_PASSWORD=$pip_password pip install --no-cache-dir -r requirements.txt -r internal_requirements.txt --user
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN ls
