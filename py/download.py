import boto3
import os
import subprocess
from filetypes import get_filetype
from helpers import check_file_exists, extract_subjects
from warnings import warn

def download(locations, filenames, inbucket, use_slurm=False, account="", requester_pays=False):
    """
    download files to your input bucket

    args:
    locations (list[str]): of file locations
    filenames (list[str]): list of file locations 
    """
    ftype, idx_ext = get_filetype(locations)

    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket="niagads-bucket", Prefix=f"{ftype}s/")

    existing_files = []
    if "Contents" in response:
        existing_files = [obj["Key"].split("/")[-1] for obj in response["Contents"]]

    failed_subjects = set()  # To store the subjects that failed to download

    for loc, file in zip(locations, filenames):
        if file in existing_files:
            print(f"File {file} already exists in the bucket. Skipping download.")
        else:
            print(f"Downloading {file} from {loc}")
            if requester_pays:
                cmd_insert = ["--request-payer", "requester"]
                slurm_insert = "--request-payer requester "
            else:
                cmd_insert = []
                slurm_insert = ""
            if not use_slurm:
                # TODO: make this work for non-S3 bucket
                cmd = ["aws", "s3" ,"cp", "copy-props", "none"] + cmd_insert + [f"{loc}", f"s3://{inbucket}/{ftype}s/{file}"]
                print(" ".join(cmd))
                try:
                    subprocess.run(cmd, check=True)
                except subprocess.CalledProcessError:
                    warn(f"Failed to download {file}!")
                    failed_subjects.add(extract_subjects(file))  # Add the subject name to the set of failed subjects
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

aws s3 cp --copy-props none {slurm_insert}"{loc}" "s3://{inbucket}/{ftype}s/{file}"
'''

                # Write the SLURM script to a file
                slurm_script_file = f"download_{file}.sh"
                with open(slurm_script_file, "w") as file:
                    file.write(slurm_script)

                # Submit the SLURM script using sbatch command
                subprocess.run(["sbatch", slurm_script_file])
                # Remove the SLURM script file after submission
                os.remove(slurm_script_file)

    # Append the failed subjects to the file
    with open("failed_downloads.txt", "a") as file:
        file.write("\n".join(failed_subjects) + "\n")
    
    # Remove duplicates from the file
    lines = set()
    with open("failed_downloads.txt", "r") as file:
        lines = file.readlines()
    with open("failed_downloads.txt", "w") as file:
        file.writelines(set(lines))
    
    print("Submissions complete.")