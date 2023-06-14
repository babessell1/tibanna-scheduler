#!/usr/bin/python

import os
import argparse
import sys
from helpers import resolve_inputs, move_logs_to_root, remove_all_inputs, remove_inputs_from_file, move_logs_to_folder
from download import download_and_index
from cost import calculate_average_cost
from launcher import make_and_launch

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="run tibanna launchers")
    parser.add_argument("--id", dest="jobid_prefix", help="Job ID")
    parser.add_argument("--ebs-size", dest="ebs_size", type=int, default=60, help="EBS size")
    parser.add_argument("--instance-types", dest="instance_types", nargs="+", help="Instance types (string or list of strings)")
    parser.add_argument("--batch-size", dest="batch_size", type=int, help="Batch size")
    parser.add_argument("--csv-file", dest="csv_file", help="CSV file path")
    parser.add_argument("--mode", dest="mode", type=str, help="download, launch, cleanup_from_file, cleanup_all, unpack_logs, cost")
    parser.add_argument("--use-slurm", dest="use_slurm", action="store_true", help="Flag to use Slurm for cost calculation")
    parser.add_argument("--account", dest="account", type=str, help="download, slurm acct name")
    parser.add_argument("--rootdir", dest="root", type=str, help="root directory for cwl and csv file lookup", default="../" )
    parser.add_argument("--inbucket", dest="inbucket", type=str, help="S3 bucket storing input files")
    parser.add_argument("--outbucket", dest="outbucket", type=str, help="S3 bucket storing output files and logs")
    parser.add_argument("--instance-cpus", dest="cores_per_inst", type=int, help="number of vCPUs for paralleizing within an AWS instance")
    parser.add_argument("--requester-pays", dest="requester_pays", action="store_true", help="Flag to indicate S3 bucket to download from is a requester-pays bucket")

    args = parser.parse_args()

    if not os.path.exists(args.root, args.csv_file):
        raise FileNotFoundError(f"{args.csv_file} not found!")
    
    if args.mode not in ["download", "launch", "cleanup_from_file", "cleanup_all", "unpack_logs", "cost"]:
        raise ValueError("Acceptable modes are: download, launch, cleanup_from_file, cleanup_all, unpack_logs, cost")

    use_slurm = True if args.mode=="download_slurm" else False
    allow_existing = True if args.mode=="cleanup_from_file" or args.mode=="cost" else False

    if args.mode=="unpack_logs":
        move_logs_to_root(args.jobid_prefix, args.outbucket)
        sys.exit(0)

    if args.mode=="cleanup_all":
        remove_all_inputs()
        # TODO: Handle logs as well
        sys.exit(0)

    # get list of len batch size of locations and their associated filenames from csv
    # if allow existing (such as for file transfer operations and cost est), this list will
    # not exclude samples which have been completed
    locations, filenames = resolve_inputs(args.csv_file, args.batch_size, allow_existing)

    if len(locations) > 0:
        if args.mode in ["download", "download_slurm"]:
            print(f"Downloading and indexing {args.batch_size} files in {args.csv_file}")
            download_and_index(locations, filenames, args.inbucket, args.use_slurm, args.account)
        elif args.mode=="launch":
            make_and_launch(args.jobid_prefix, filenames, args.instance_types, args.inbucket, args.outbucket, cores_per_inst=args.cores_per_inst, ebs_size=args.ebs_size, use_slurm=args.use_slurm, account=args.account)
        elif args.mode=="cleanup_from_file":
            print(f"Removing inputs  {args.batch_size} files in {args.csv_file}")
            remove_inputs_from_file(filenames, args.inbucket)
            move_logs_to_folder(args.jobid_prefix, args.outbucket)
        elif args.mode=="cost":
            calculate_average_cost(args.jobid_prefix, args.use_slurm, args.account, filenames, args.cores_per_inst)
    else:
        print("Nothing to be done!")
    
    sys.exit(0)

    