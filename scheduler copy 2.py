#!/usr/bin/python

import boto3
import os
import argparse
import csv
import json

cores_per_inst = 2
inbucket = "niagads-bucket"
outbucket = "niagads-out-bucket"

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
    idx_filenames = [cram + ".crai" for cram in filenames]
    grouped_idx = [idx_filenames[i:i+items_per_list] for i in range(0, len(filenames), items_per_list)]
    return prepend_path(grouped_crams, "crams/"), prepend_path(grouped_idx, "cramsidx/")

def make_and_launch(filenames, instance_types, cores_per_inst, ebs_size, instance_id):
    cnt = 0
    for crams, cramsidx in zip(*group_inputs(filenames, cores_per_inst)):
        cnt += 1
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
                            "bucket_name": "{inbucket}",
                            "object_key": {crams}
                        }},
                        "fasta": {{
                            "bucket_name": "niagads-bucket",
                            "object_key": "references/GRCh38_full_analysis_set_plus_decoy_hla.fa.gz",
                            "unzip": "gz"
                        }},
                        "cramsidx": {{
                            "bucket_name": "{inbucket}",
                            "object_key": {cramsidx}
                        }},
                        "fastaidx": {{
                            "bucket_name": "{inbucket}",
                            "object_key": "references/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai.gz",
                            "unzip": "gz"
                        }}
                    }},
                    "output_S3_bucket": "{outbucket}",
                    "output_target": {{
                        "out_str": "output/strling/"
                    }}
                }},
                "config": {{
                    "ebs_size": {ebs_size},
                    "instance_type": {json.dumps(instance_types)},
                    "EBS_optimized": true,
                    "password": "corndog",
                    "log_bucket": "{outbucket}",
                    "spot_instance": "true",
                    "key_name": "big-wgs-key"
                }}
            }}
        '''.replace("'", '"')
        print(job_description)
        with open("job_description.json", "w") as job_description_file:
            job_description_file.write(job_description)
        os.system(f"tibanna run_workflow --input-json=job_description.json --do-not-open-browser --jobid={instance_id}.{cnt}")

def get_subject_completed_set(outbucket):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=outbucket, Prefix='output/completed/')
    completed_set = set()
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.txt'):
                subject = os.path.splitext(os.path.basename(obj['Key']))[0]
                completed_set.add(subject)
    return completed_set

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="run tibanna launchers")
    parser.add_argument("--id", dest="instance_id", help="Instance ID")
    parser.add_argument("--ebs-size", dest="ebs_size", type=int, default=60, help="EBS size")
    parser.add_argument("--instance-types", dest="instance_types", nargs="+", help="Instance types (string or list of strings)")
    parser.add_argument("--batch-size", dest="batch_size", type=int, help="Batch size")
    parser.add_argument("--csv-file", dest="csv_file", help="CSV file path")
    args = parser.parse_args()

    ebs_size = args.ebs_size

    instance_types = args.instance_types
    if instance_types is None:
        instance_types = "t3.large"

    csv_file = args.csv_file
    batch_size = args.batch_size

    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        locations = []
        completed_set = get_subject_completed_set(outbucket)
        for row in reader:
            print(row)
            print("comp set: ", completed_set)
            if row['Subject'] not in completed_set:
                locations.append(row['location'])
            if len(locations) == batch_size:
                break

    # Adjusting locations to be a multiple of cores_per_inst
    locations = locations[:len(locations) - (len(locations) % cores_per_inst)]
    filenames = [loc.split("/")[-1] for loc in locations]

    if len(locations)>0:
        make_and_launch(filenames, instance_types, cores_per_inst, ebs_size, args.instance_id)
    else:
        print("Nothing to be done!")
