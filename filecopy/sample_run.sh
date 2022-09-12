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

#docker build -t indoc/filecopy:v0.1 .

SRC=/home/sliang/test.py
DES=/tmp
LOGFILE=/tmp/dcmpipeline.log
if [ ! -f $LOGFILE ]; then 
    touch $LOGFILE
fi

UID=$(id -u)
GID=$(id -g)
docker run \
    --user $UID:$GID \
    -v /etc/group:/etc/group:ro \
    -v /etc/passwd:/etc/passwd:ro \
    -v /etc/shadow:/etc/shadow:ro \
    -v ${SRC}:${SRC}:ro \
    -v ${DES}:${DES} \
    -v ${LOGFILE}:${LOGFILE} \
    indoc/filecopy:v0.1 \
    rsync -avvv \
    $SRC $DES \
    --log-file=$LOGFILE


