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

FROM ubuntu:18.04
LABEL maintainer="Shuai Liang <sliang@indocresearch.org>"

ARG pip_username
ARG pip_password

RUN sed 's/main$/main universe/' -i /etc/apt/sources.list
RUN apt-get update && apt-get install -y python3-pip python3-dev build-essential \
    default-jre-headless locales && rm -rf /var/lib/apt/lists/*
RUN pip3 install --upgrade pip
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
COPY ./ /dcm
RUN PIP_USERNAME=$pip_username PIP_PASSWORD=$pip_password pip3 install -r /dcm/scripts/requirements.txt  -r /dcm/scripts/internal_requirements.txt
RUN chmod 555 -R /dcm

WORKDIR /dcm
