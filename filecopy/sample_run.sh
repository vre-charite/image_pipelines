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


