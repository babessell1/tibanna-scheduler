#!/usr/bin/python

import csv
import argparse
import os
import boto3

def get_s3_file_size(s3_client, s3_bucket, s3_object_key):
    try:
        response = s3_client.head_object(Bucket=s3_bucket, Key=s3_object_key)
        return response["ContentLength"]
    except Exception as e:
        print(f"Failed to get file size for {s3_object_key}: {str(e)}")
        return None

def bind_csv_files(main_file, loc_file, output_file, aws_access_key_id, aws_secret_access_key, s3_bucket):
    # Read the main file
    with open(main_file, "r") as main_csv:
        main_reader = csv.DictReader(main_csv)
        main_data = list(main_reader)

    # Read the loc file
    with open(loc_file, "r") as loc_csv:
        loc_reader = csv.reader(loc_csv)
        loc_data = list(loc_reader)

    # Initialize AWS S3 client
    s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)


    # Bind the data based on matching substrings
    matched_data = []
    unmatched_subjects = set()
    for row in main_data:
        subject = row["Subject"].strip()
        matching_location = None
        for loc_row in loc_data:
            location = loc_row[0].strip()
            if subject in location:
                matching_location = location
                break
        if matching_location:
            row["location"] = matching_location
            # Check the size of the file in S3
            s3_object_key = matching_location  # Assuming the S3 key is the same as the location
            file_size = get_s3_file_size(s3, s3_bucket, s3_object_key)
            if file_size is not None:
                row["size"] = file_size

            matched_data.append(row)
        else:
            unmatched_subjects.add(subject)

    # Sort the matched data by size in ascending order
    matched_data.sort(key=lambda x: x.get("size", 0))

    # Write the matched data to the output file
    with open(output_file, "w", newline="") as output_csv:
        fieldnames = main_reader.fieldnames + ["location"]
        writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(matched_data)

    # Write the unmatched subjects to a separate CSV file
    unmatched_file = os.path.splitext(output_file)[0] + "_not_found.csv"
    with open(unmatched_file, "w", newline="") as unmatched_csv:
        writer = csv.writer(unmatched_csv)
        writer.writerow(["Subject"])
        writer.writerows([[subject] for subject in unmatched_subjects])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bind Niagads sample info file with location file")
    parser.add_argument("--main_file", dest="main_file", help="Path to the main CSV file")
    parser.add_argument("--loc_file", dest="loc_file", help="Path to the location CSV file")
    parser.add_argument("--output_file", dest="output_file", help="Path to the output CSV file")
    parser.add_argument("--aws_access_key_id", dest="aws_access_key_id", help="AWS Access Key ID")
    parser.add_argument("--aws_secret_access_key", dest="aws_secret_access_key", help="AWS Secret Access Key")
    parser.add_argument("--s3_bucket", dest="s3_bucket", help="S3 Bucket name")
    args = parser.parse_args()
    bind_csv_files(args.main_file, args.loc_file, args.output_file, args.aws_access_key_id, args.aws_secret_access_key, args.s3_bucket)
