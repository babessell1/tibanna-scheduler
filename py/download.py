import boto3
import os
import subprocess
from helpers import check_file_exists

def download_and_index(locations, filenames, inbucket, use_slurm=False, account="", requester_pays=False):
    """
    download cram files from NIAGADS and index locally or using slurm

    args:
    locations (list[str]): of file locations
    filenames (list[str]): list of file locations 
    """
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
            if requester_pays:
                cmd_insert = ["--request-payer", "requester"]
                slurm_insert = "--request-payer requester "
            else:
                cmd_insert = []
                slurm_insert = ""
            if not use_slurm:
                cmd = ["aws", "s3" ,"cp"] + cmd_insert + [f"{loc}", f"s3://{inbucket}/crams/{file}"]
                subprocess.run(cmd, check=True)
                subprocess.run(["samtools", "index", f"s3://{inbucket}/crams/{file}"], check=True)
                subprocess.run(["aws", "s3", "mv", f"s3://{inbucket}/crams/{file}.crai", f"s3://{inbucket}/cramsidx/{file}.crai"], check=True)
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

aws s3 cp {slurm_insert}"{loc}" "s3://{inbucket}/crams/{file}"

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