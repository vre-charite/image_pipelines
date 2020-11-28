docker build -t indoc/dcmedit:v0.1 .

#ZIPFILE=/home/sliang/test/GENERATE/101_DTI.zip
ZIPFILE=/home/sliang/test/generate/d神.zip
OUTDIR=/mnt/data/dcmedit
WORKDIR=/mnt/data/dcmtmp
LOGFILE=/mnt/data/dcmtmp/dcmpipeline.log
PROJ=generate1
SUBJ=GEN_SUBJ_006
SRCDIR=/home/sliang/indoc_pipelines/dcmedit
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
    -v ${ZIPFILE}:${ZIPFILE}:ro \
    -v ${OUTDIR}:${OUTDIR} \
    -v ${WORKDIR}:${WORKDIR} \
    -v ${LOGFILE}:${LOGFILE} \
    -v ${SRCDIR}:/dcm \
    indoc/dcmedit:v0.1 \
    /usr/bin/python3 scripts/dcm_edit.py \
    -i $ZIPFILE -o $OUTDIR -t $WORKDIR -l $LOGFILE \
   --use-default-anonymization

