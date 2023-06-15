import time
import re
import subprocess
import random
from helpers import move_logs_to_root, group_inputs, move_logs_to_folder, get_unique_job_ids_from_s3_bucket

def calculate_average_cost(jid, outbucket, use_slurm, account, filenames, cores_per_inst):
    """
    calculate average cost of job
    TODO: slurm is needed to measure ALL but is buggy due to throttling errors right now
    """
    # Move jib id associated logs to root (necessary for tibanna to find)
    move_logs_to_root(jid, outbucket)

    # get unique job names with jid prefix and shuffle
    job_ids = get_unique_job_ids_from_s3_bucket(outbucket, jid)
    random.shuffle(job_ids)

    # Extract the job IDs with matching prefix and submit Slurm jobs for cost calculation
    cost_job_ids = []
    
    # Extract the job IDs with matching prefix and calculate their costs
    total_cost = 0
    num_jobs = 0

    for job_id in job_ids:

        if not use_slurm:
            try:
                cost_output = subprocess.check_output(['tibanna', 'cost', '-j', job_id ]).decode('utf-8')
                print(f"{job_id}: ", cost_output)
                cost_match = re.search(r"(\d+\.\d+)", cost_output)
                if cost_match:
                    cost = float(cost_match.group(1))
                    if cost > 0:
                        total_cost += cost
                        num_jobs += 1
                        if num_jobs >= 20:
                            break  # it will take all day to do all of them. Just use a sample
            except:
                 print(f"{job_id} logs not found!")
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
    move_logs_to_folder(jid, outbucket)

    return average_cost
