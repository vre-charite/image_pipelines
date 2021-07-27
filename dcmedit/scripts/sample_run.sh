ZIPFILE=/home/gregmccoy/Programming/image_pipelines/sample/greg_sampledicom12.zip
OUTDIR=/mnt/data/dcmedit
WORKDIR=/mnt/data/dcmtmp
LOGFILE=/mnt/data/dcmtmp/dcmpipeline.log
PROJ=GENERATE
SUBJ=GEN_SUBJ_006
#if [ ! -f $LOGFILE ]; then 
#    touch $LOGFILE
#fi
#UID=$(id -u)
#GID=$(id -g)

#    --user $UID:$GID \
#    -v /etc/group:/etc/group:ro \
#    -v /etc/passwd:/etc/passwd:ro \
#    -v /etc/shadow:/etc/shadow:ro \
#    -v ${LOGFILE}:${LOGFILE} \

#docker run \
#    -v ${ZIPFILE}:${ZIPFILE}:ro \
#    -v ${OUTDIR}:${OUTDIR} \
#    -v ${WORKDIR}:${WORKDIR} \
#    indoc/dcmedit:v0.1 \
python dcm_edit.py -i $ZIPFILE -o $OUTDIR -t $WORKDIR -p $PROJ -s $SUBJ

