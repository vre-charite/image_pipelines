
DATASET=/mnt/data/ds002080/
OUTPUTDIR=/mnt/data/ds001226o/

docker run -i --rm \
      -v ${DATASET}:/bids_dataset \
      -v ${OUTPUTDIR}:/output \
      thevirtualbrain/tvb-pipeline-sc:1.1 \
      /bids_dataset /output participant1 --participant_label CON03 \
       -skip  --output_verbosity 2 --template_reg ants --n_cpus 4 --debug
        

