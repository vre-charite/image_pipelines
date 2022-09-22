<!--
 Copyright 2022 Indoc Research
 
 Licensed under the EUPL, Version 1.2 or – as soon they
 will be approved by the European Commission - subsequent
 versions of the EUPL (the "Licence");
 You may not use this work except in compliance with the
 Licence.
 You may obtain a copy of the Licence at:
 
 https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
 
 Unless required by applicable law or agreed to in
 writing, software distributed under the Licence is
 distributed on an "AS IS" basis,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 express or implied.
 See the Licence for the specific language governing
 permissions and limitations under the Licence.
 
-->

# Dicom header editor

## Build docker

docker build -t indoc/dcmedit:v0.1 .

## Run docker

### a) Suppose the input file and output dir are:

FIN=/path/{Project_id}/path/fname.zip  # project ID has to be included in the path, which will be used to trim the filename to session name
ODIR=/dirA/dirB
WDIR=/dirA/tmpdir
LOGFILE=/LOGS/dcm.log # file has to exist

### b) Run docker using the command:

docker run -v $FIN:$FIN -v $DIR:$DIR -v $WDIR:$WDIR -v $LOGFILE:$LOGFILE indoc/dcmedit:v0.1 /usr/bin/python3 scripts/dcm_edit.py -i $FIN -o $DIR -t $WORKDIR -l $LOGFILE

### c) Edited file will be: /dirA/dirB/fname_edited.zip

### See file sample_run.sh for running it as current user.
