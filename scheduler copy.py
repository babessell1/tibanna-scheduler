#!/usr/bin/python

import boto3
import os
import argparse

########## parameters: ##########
ebs_size = 60
instance_types = "t3.large"
cores_per_inst = 2
inbucket = "s3://niagads-bucket"
#################################


# TODO: read these from file

locations = [
   "s3://wanglab-dss-share/distribution/adsp/cram/snd10011/A-ROS-RS000011-BL-RUS-a0608_vcpa1.1.cram",
   "s3://wanglab-dss-share/distribution/adsp/cram/snd10011/A-ROS-RS000014-BR-RUS-a0610_vcpa1.1.cram"
]

filenames = [loc.split("/")[-1] for loc in locations]

def prepend_path(nested_list, path):
    if isinstance(nested_list, str):
       return path + nested_list
    else:
        return [prepend_path(item, path) for item in nested_list]

def basename(nested_list):
    if isinstance(nested_list, str):
       return os.path.splitext(os.path.basename(nested_list))[0]
    else:
        return [basename(item) for item in nested_list]

def group_inputs(filenames, items_per_list):
    grouped_crams = [filenames[i:i+items_per_list] for i in range(0, len(filenames), items_per_list)]
    #grouped_crams = [[[filename] for filename in filenames[i:i+items_per_list]] for i in range(0, len(filenames), items_per_list)]
    idx_filenames = [cram + ".crai" for cram in filenames]
    grouped_idx = [idx_filenames[i:i+items_per_list] for i in range(0, len(filenames), items_per_list)]
    return prepend_path(grouped_crams, "crams/"), prepend_path(grouped_idx, "cramsidx/")

def make_and_launch(filenames, instance_types, cores_per_inst, ebs_size, instance_id):
  cnt=0
  for crams, cramsidx in zip( *group_inputs(filenames, cores_per_inst) ):
    cnt+=1
    print(crams)
    print(cramsidx)

    job_description = f'''
        {{
            "args": {{
                "app_name": "big-sv",
                "cwl_directory_local": "/home/bbessell/bigsv/cwl/",
                "cwl_main_filename": "call_strling.cwl",
                "cwl_version": "v1",
                "input_files": {{
                    "crams": {{
                        "bucket_name": "niagads-bucket",
                        "object_key": {crams}
                    }},
                    "fasta": {{
                        "bucket_name": "niagads-bucket",
                        "object_key": "references/GRCh38_full_analysis_set_plus_decoy_hla.fa.gz",
                        "unzip": "gz"
                    }},
                    "cramsidx": {{
                        "bucket_name": "niagads-bucket",
                        "object_key": {cramsidx}
                    }},
                    "fastaidx": {{
                        "bucket_name": "niagads-bucket",
                        "object_key": "references/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai.gz",
                        "unzip": "gz"
                    }}
                }},
                "output_S3_bucket": "niagads-out-bucket",
                "output_target": {{
                    "out_str": "output/strling/"
                }}
            }},
            "config": {{
                "ebs_size": {ebs_size},
                "instance_type": "{instance_types}",
                "EBS_optimized": true,
                "password": "corndog",
                "log_bucket": "niagads-out-bucket",
                "spot_instance": "true",
                "key_name": "big-wgs-key"
            }}
        }}
    '''.replace("'", '"')
    print(job_description)
    with open("job_description.json", "w") as job_description_file:
      job_description_file.write(job_description)
    os.system(f"tibanna run_workflow --input-json=job_description.json --do-not-open-browser --jobid={instance_id}.{cnt}")

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="run tibanna launchers")
    parser.add_argument("--id", dest="instance_id", help="Instance ID")
    args = parser.parse_args()
    make_and_launch(filenames, instance_types, cores_per_inst, ebs_size, args.instance_id)