#!/usr/bin/python

import boto3
import os
import argparse
import csv
import json
import re
import subprocess
import sys
import time

cores_per_inst = 2
inbucket = "niagads-bucket"
outbucket = "niagads-out-bucket"

def resolve_inputs(csv_file, batch_size, allow_existing=False):
        """
        takes a csv file with columns Location, Subject to populate lists of each one
        constrained by other args

        args
        csv_file (str): must have columns "Location, Subject"
        batch_size (int): max size to process at a time
        allow_existing (bool): excludes samples whose outputs are in the outbucket if false
        """
        with open(csv_file, 'r') as file:
            reader = csv.DictReader(file)
            locations = []
            completed_set = get_subject_completed_set(outbucket) if not allow_existing else {}

            print("c set: ", len(completed_set))
            for row in reader:
                if row['Subject'] not in completed_set:
                    locations.append(row['location'])
                else:
                    print(row['Subject'], " has already been called, skipping!")
                if len(locations) == batch_size:
                    break

        # Adjusting locations to be a multiple of cores_per_inst
        locations = locations[:len(locations) - (len(locations) % cores_per_inst)]
        filenames = [loc.split("/")[-1] for loc in locations]
        print("files: ", len(filenames))

        return locations, filenames

def prepend_path(nested_list, path):
    """
    adds a specified path to a nested list of files

    args
    nested_list (list): nested list of any size
    path (str): path to prepend
    """
    if isinstance(nested_list, str):
        return path + nested_list
    else:
        return [prepend_path(item, path) for item in nested_list]

def basename(nested_list):
    """
    convert nested list of files to nest list of their basenames
    """
    if isinstance(nested_list, str):
        return os.path.splitext(os.path.basename(nested_list))[0]
    else:
        return [basename(item) for item in nested_list]

def group_inputs(filenames, items_per_list):
    """
    takes a list of filenames and desired nested list size
    and converts the flat list to nested

    args
    filenames (list[str]): flat list of filenames
    items_per_list (int): size of nested list
    """
    grouped_crams = [filenames[i:i+items_per_list] for i in range(0, len(filenames), items_per_list)]
    idx_filenames = [cram + ".crai" for cram in filenames]
    grouped_idx = [idx_filenames[i:i+items_per_list] for i in range(0, len(filenames), items_per_list)] 

    return basename(grouped_crams), prepend_path(grouped_crams, "crams/"), prepend_path(grouped_idx, "cramsidx/")


def make_and_launch(filenames, instance_types, cores_per_inst, ebs_size, instance_id, use_slurm, account):
    """
    Create a tibanna job description json file and submit it.
    TODO: unhardcode the template so others can be used

    args
    filenames (list[str]): flat list of filenames 

    """
    cnt = 0
    for snames, crams, cramsidx in zip(*group_inputs(filenames, cores_per_inst)):
        cnt += 1
        job_description = f'''
            {{
                "args": {{
                    "app_name": "call-strling",
                    "cwl_directory_local": "cwl/",
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
                        "out": "output/strling/"
                }},
                    "secondary_output_target": {{}}
                }},
                "config": {{
                    "ebs_size": {ebs_size},
                    "instance_type": {json.dumps(instance_types)},
                    "EBS_optimized": true,
                    "password": "",
                    "log_bucket": "niagads-out-bucket",
                    "spot_instance": true,
                    "key_name": "big-wgs-key"
                }}
            }}
        '''.replace("'", '"')
        tag = ".".join(snames)
        job_id = f"{instance_id}.{tag}.{cnt}"
        with open(f"slurm/{job_id}_job_description.json", "w") as job_description_file:
            job_description_file.write(job_description)

        if use_slurm:
            os.system(f'sbatch -J launch_{job_id} -o logs/launch_{job_id}.out -e logs/launch_{job_id}.err -A {account} --mem=100M -c 1 --wrap="tibanna run_workflow --input-json=slurm/{job_id}_job_description.json --do-not-open-browser --jobid={job_id}"')
        else:
            os.system(f'tibanna run_workflow --input-json="slurm/{job_id}_job_description.json" --do-not-open-browser --jobid={job_id}')


def extract_subjects(string):
    pattern = re.compile(r'[A-Za-z-]+RS[\d-]+(?<!-)')
    matches = pattern.findall(string)

    return matches


def get_subject_completed_set(outbucket):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=outbucket, Prefix='/mnt/data1/out/')
    completed_set = set()
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.tar'):
                subj = extract_subjects(str(obj['Key']))
                for s in subj:
                    completed_set.add(s)

    return completed_set


def check_file_exists(bucket_name, file_key):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=file_key):
        if obj.key == file_key:
            return True
    
    return False


def download_and_index(locations, filenames, use_slurm, account):
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket="niagads-bucket", Prefix="crams/")

    existing_files = []
    if "Contents" in response:
        existing_files = [obj["Key"].split("/")[-1] for obj in response["Contents"]]

    for loc, file in zip(locations, filenames):
        if file in existing_files:
            print(f"File {file} already exists in the bucket. Skipping download.")
            if check_file_exists(inbucket, f"cramsidx/{file}.crai"):
                print(f"File {file} already indexed in the bucket. Skipping indexing.")
            else:
                print(f"Indexing: s3://{inbucket}/crams/{file}")
                if not use_slurm:
                    subprocess.run(["samtools", "index", f"s3://{inbucket}/crams/{file}"], check=True)
                    subprocess.run(["aws", "s3", "mv", f"s3://{inbucket}/crams/{file}.crai", f"s3://{inbucket}/cramsidx/{file}.crai"], check=True)
                else:
                   slurm_script = f'''#!/bin/bash
#SBATCH --job-name=index_{file}
#SBATCH --account={account}
#SBATCH --output=logs/index_{file}.out
#SBATCH --error=logs/index_{file}.err
#SBATCH --time=1:00:00
#SBATCH --ntasks=1
#SBATCH  --mem-per-cpu=2G
#SBATCH --cpus-per-task=1

samtools index "s3://{inbucket}/crams/{file}"
aws s3 mv "s3://{inbucket}/crams/{file}.crai" "s3://{inbucket}/cramsidx/{file}.crai"
                '''

                # Write the SLURM script to a file
                slurm_script_file = f"index_{file}.sh"
                with open(slurm_script_file, "w") as file:
                    file.write(slurm_script)

                # Submit the SLURM script using sbatch command
                subprocess.run(["sbatch", slurm_script_file])
                # Remove the SLURM script file after submission
                os.remove(slurm_script_file)
 
        else:
            print(f"Downloading {file} from {loc}")
            if not use_slurm:
                subprocess.run(["aws", "s3" ,"cp", "--request-payer", "requester", f"{loc}", f"s3://{inbucket}/crams/{file}"], check=True)
                #downloader.communicate()
                subprocess.run(["samtools", "index", f"s3://{inbucket}/crams/{file}"], check=True)
                #indexer.communicate()
                subprocess.run(["aws", "s3", "mv", f"s3://{inbucket}/crams/{file}.crai", f"s3://{inbucket}/cramsidx/{file}.crai"], check=True)
                #mover.communicate()
            else:
                slurm_script = f'''#!/bin/bash
#SBATCH --job-name=download_{file}
#SBATCH --account={account}
#SBATCH --output=logs/download_{file}.out
#SBATCH --error=logs/download_{file}.err
#SBATCH --time=1:00:00
#SBATCH --ntasks=1
#SBATCH  --mem-per-cpu=2G
#SBATCH --cpus-per-task=1

aws s3 cp --request-payer requester "{loc}" "s3://{inbucket}/crams/{file}"

samtools index "s3://{inbucket}/crams/{file}"
aws s3 mv "s3://{inbucket}/crams/{file}.crai" "s3://{inbucket}/cramsidx/{file}.crai"
'''

            # Write the SLURM script to a file
            slurm_script_file = f"download_{file}.sh"
            with open(slurm_script_file, "w") as file:
                file.write(slurm_script)

            # Submit the SLURM script using sbatch command
            subprocess.run(["sbatch", slurm_script_file])
            # Remove the SLURM script file after submission
            os.remove(slurm_script_file)

    print("Submissions complete.")


def remove_inputs_from_file(filenames):
    s3 = boto3.client('s3')
    for file in filenames:
        s3.delete_object(Bucket=inbucket, Key=f"crams/{file}")
        s3.delete_object(Bucket=inbucket, Key=f"cramsidx/{file}.crai")
    print("Files deleted successfully.")


def remove_all_inputs():
    s3 = boto3.client('s3')
    for dir in ["cramsidx", "crams"]:
        response = s3.list_objects_v2(Bucket=inbucket, Prefix=dir)
        if 'Contents' in response:
            # Delete each file in the folder
            objects = [{'Key': obj['Key']} for obj in response['Contents']]
            s3.delete_objects(Bucket=inbucket, Delete={'Objects': objects})

    print("Successfully removed all input files!")


def move_logs_to_folder(jid):
    s3 = boto3.client('s3')

    # Create the folder in the root of the S3 bucket
    s3.put_object(Bucket=outbucket, Key=f"{jid}/")

    # List all objects in the bucket with the specified prefix
    response = s3.list_objects_v2(Bucket=outbucket, Prefix=f"{jid}.")

    if 'Contents' in response:
        # Move each file with the specified prefix to the folder
        for file in response['Contents']:
            file_name = file['Key']
            new_key = f"{jid}/{file_name}"
            s3.copy_object(Bucket=outbucket, Key=new_key, CopySource={'Bucket': outbucket, 'Key': file_name})
            s3.delete_object(Bucket=outbucket, Key=file_name)

    print(f"{jid} logs consolidated successfully.")


def move_logs_to_root(jid):
    s3 = boto3.client('s3')

    # List all objects in the specified folder
    response = s3.list_objects_v2(Bucket=outbucket, Prefix=f"{jid}/{jid}.")

    if 'Contents' in response:
        # Move each file to the root directory
        for file in response['Contents']:
            file_name = file['Key']
            new_key = os.path.basename(file_name)
            s3.copy_object(Bucket=outbucket, Key=new_key, CopySource={'Bucket': outbucket, 'Key': file_name})
            s3.delete_object(Bucket=outbucket, Key=file_name)

    print(f"{jid} logs moved to root directory successfully.")


def calculate_average_cost(jid, use_slurm, account, filenames, cores_per_inst):
    # Move jib id associated logs to root (necessary for tibanna to find)
    move_logs_to_root(jid)

    # Extract the job IDs with matching prefix and submit Slurm jobs for cost calculation
    cost_job_ids = []
    
    # Extract the job IDs with matching prefix and calculate their costs
    total_cost = 0
    num_jobs = 0
    cnt = 0

    for snames, crams, cramsidx in zip(*group_inputs(filenames, cores_per_inst)):
        cnt += 1
        tag = ".".join(snames)
        job_id = f"{jid}.{tag}.{cnt}"
        if not use_slurm:
            cost_output = subprocess.check_output(['tibanna', 'cost', '-j', job_id ]).decode('utf-8')
            print(f"{job_id}: ", cost_output)
            cost_match = re.search(r"(\d+\.\d+)", cost_output)
            if cost_match:
                cost = float(cost_match.group(1))
                if cost > 0:
                    total_cost += cost
                    num_jobs += 1
                    if num_jobs == 10:
                        break  # it will take all day to do all of them. Just use a sample
        else:
            print(f"Submitting Slurm job for {job_id}")
            slurm_cmd = f'sbatch -J cost_{job_id} -o logs/cost_{job_id}.out -e logs/cost_{job_id}.err -A {account} --mem=100M -c 1 --wrap="tibanna cost -j {job_id}"'
            time.sleep(.5)  # prevent throttling error
            subprocess.call(slurm_cmd, shell=True)
            cost_job_ids.append(job_id)
    
    if use_slurm:
        # Wait until all are done
        while True:
            time.sleep(5)
            try:
                output = subprocess.check_output(['squeue', '-h', '-n', ','.join([f"cost_{job}" for job in cost_job_ids]), '-t', 'COMPLETED']).decode('utf-8')
            except subprocess.CalledProcessError:
                # Handle the error when checking the Slurm job status
                print(f"An error occurred while checking the Slurm job status ({job_id})")
                # TODO: this event is either from throttling or from a missing log file. Add a way to check if files did not complete without having to use launch again.
                continue

            if not output:
                break

        # Read the output files of the Slurm jobs to obtain costs
        total_cost = 0
        num_jobs = 0

        for job_id in cost_job_ids:
            cost_output = subprocess.check_output(['cat', f'cost_{job_id}.out']).decode('utf-8')
            print(f"{job_id}: ", cost_output)
            cost_match = re.search(r"(\d+\.\d+)", cost_output)
            if cost_match:
                cost = float(cost_match.group(1))
                if cost > 0:
                    total_cost += cost
                    num_jobs += 1

    # Calculate the average cost
    average_cost = total_cost / num_jobs if num_jobs > 0 else 0
    print(f"Average Cost: {average_cost}")

    # move jid associated logs back to folder
    move_logs_to_folder(jid)

    return average_cost


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="run tibanna launchers")
    parser.add_argument("--id", dest="instance_id", help="Instance ID")
    parser.add_argument("--ebs-size", dest="ebs_size", type=int, default=60, help="EBS size")
    parser.add_argument("--instance-types", dest="instance_types", nargs="+", help="Instance types (string or list of strings)")
    parser.add_argument("--batch-size", dest="batch_size", type=int, help="Batch size")
    parser.add_argument("--csv-file", dest="csv_file", help="CSV file path")
    parser.add_argument("--mode", dest="mode", type=str, help="download, launch, cleanup_from_file, cleanup_all, unpack_logs, cost")
    parser.add_argument("--use-slurm", dest="use_slurm", action="store_true", help="Flag to use Slurm for cost calculation")
    parser.add_argument("--account", dest="account", type=str, help="download, slurm acct name")

    args = parser.parse_args()

    if not os.path.exists(args.csv_file):
        raise FileNotFoundError(f"{args.csv_file} not found!")
    
    
    if args.mode not in ["download", "launch", "cleanup_from_file", "cleanup_all", "unpack_logs", "cost"]:
        raise ValueError("Acceptable modes are: download, launch, cleanup_from_file, cleanup_all, unpack_logs, cost")

    use_slurm = True if args.mode=="download_slurm" else False
    allow_existing = True if args.mode=="cleanup_from_file" or args.mode=="cost" else False

    if args.mode=="unpack_logs":
        move_logs_to_root(args.instance_id)
        sys.exit(1)

    if args.mode=="cleanup_all":
        remove_all_inputs()
        # TODO: Handle logs as well
        sys.exit(1)

    # get list of len batch size of locations and their associated filenames from csv
    # if allow existing (such as for file transfer operations and cost est), this list will
    # not exclude samples which have been completed
    locations, filenames = resolve_inputs(args.csv_file, args.batch_size, allow_existing)

    if len(locations) > 0:
        if args.mode in ["download", "download_slurm"]:
            print(f"Downloading and indexing {args.batch_size} files in {args.csv_file}")
            download_and_index(locations, filenames, args.use_slurm, args.account)
        elif args.mode=="launch":
            make_and_launch(filenames, args.instance_types, cores_per_inst, args.ebs_size, args.instance_id, args.use_slurm, args.account)
        elif args.mode=="cleanup_from_file":
            print(f"Removing inputs  {args.batch_size} files in {args.csv_file}")
            remove_inputs_from_file(filenames)
            move_logs_to_folder(args.instance_id)
        elif args.mode=="cost":
            calculate_average_cost(args.instance_id, args.use_slurm, args.account, filenames, cores_per_inst)
        sys.exit(1)
    else:
        print("Nothing to be done!")
    
