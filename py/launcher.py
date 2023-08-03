import os
import json
from helpers import group_inputs
from job_templates import get_job_templates

def make_and_launch(job_key, jobid_prefix, filenames, instance_types, inbucket, outbucket, cores_per_inst=2, ebs_size=60, use_slurm=False, account=""):
    """
    Create a tibanna job description json file and submit it.
    TODO: unhardcode the template so others can be used
    IDEA: use seperate file to define job descriptions, store a dictionary and import

    args
    filenames (list[str]): flat list of filenames
    instance_types (list[str]): list of aws instances to use
    cores_per_inst (int): number of cores per instance
    ebs_size (int): needed storage size
    jobid_prefix (str): job id prefix to assign each new instance
    use_slurm (bool): parallelize launching on slurm (takes very long for large batches otherwise)
    account (str): slurm account to charge to
    """
    cnt = 0
    if not os.path.exists("job_desc"):
        os.makedirs("job_desc")
    for subject_names, subject_ids, inputs, inputs_idx in zip(*group_inputs(filenames, cores_per_inst)):
        cnt += 1
        job_description = get_job_templates(inbucket, outbucket, inputs, inputs_idx, ebs_size, instance_types)
        tag = "._.".join(subject_ids)
        job_id = f"{jobid_prefix}.{tag}.{cnt}"
        with open(f"job_desc/{job_id}_job_description.json", "w") as job_description_file:
            #print(job_description[job_key])
            job_description_file.write(job_description[job_key])

        if use_slurm:
            os.system(f'sbatch -J launch_{job_id} -o logs/launch_{job_id}.out -e logs/launch_{job_id}.err -A {account} --mem=100M -c 1 --time=2 --wrap="tibanna run_workflow --input-json=job_desc/{job_id}_job_description.json --do-not-open-browser --jobid={job_id}"')
            #os.system(f'sbatch -J launch_{job_id} -o logs/launch_{job_id}.out -e logs/launch_{job_id}.err -A {account} --mem=100M -c 1 --wrap="tibanna run_workflow --input-json=job_desc/{job_id}_job_description.json --do-not-open-browser --jobid={job_id}"')
        else:
            os.system(f'tibanna run_workflow --input-json="job_desc/{job_id}_job_description.json" --do-not-open-browser --jobid={job_id}')
