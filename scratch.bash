python py/scheduler.py \
    --ebs-size 70 \
    --account remills99 \
    --inbucket niagads-bucket \
    --outbucket niagads-out-bucket \
    --instance-cpus 2 \
    --requester-pays \
    --rerun-failed \
    --use-slurm \
    --instance-types "c6i.large" \
    --job-key "call_strling"  \
    --batch-size 250 \
    --csv "ADNI_MIA_PR1066_VAN_StEP_manifest.csv" \
    --mode "download"  \
    --id "1_strling_ADNI_MIA_PR1066_VAN_StEP"

python py/scheduler.py \
    --ebs-size 70 \
    --account remills99 \
    --inbucket niagads-bucket\
    --outbucket niagads-ehdn \
    --instance-cpus 2 \
    --requester-pays \
    --rerun-failed \
    --use-slurm \
    --instance-types "m7i-flex.large" \
    --job-key "call_ehdn"  \
    --batch-size 250 \
    --csv "ADNI_MIA_PR1066_VAN_StEP_manifest.csv"  \
    --mode "launch" \
    --id "1_ehdn_ADNI_MIA_PR1066_VAN_StEP"


     