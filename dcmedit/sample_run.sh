docker build -t indoc/dcmedit:v0.1 .

ZIPFILE=/home/sliang/test/GENERATE/101_DTI.zip
OUTDIR=/mnt/data/dcmedit
WORKDIR=/mnt/data/dcmtmp
LOGFILE=/mnt/data/dcmtmp/dcmpipeline.log
PROJ=GENERATE
SUBJ=GEN_SUBJ_006
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
    indoc/dcmedit:v0.1 \
    /usr/bin/python3 scripts/dcm_edit.py \
    -i $ZIPFILE -o $OUTDIR -t $WORKDIR -l $LOGFILE \
    -p $PROJ -s $SUBJ

