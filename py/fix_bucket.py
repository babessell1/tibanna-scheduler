import os
import tarfile
import tempfile
import boto3

def handle_2_sample_case(sample_set, samples, bucket_name, object_key, obj, s3, bucket_directory):
    sample_name1 = samples[0]
    sample_name2 = samples[1]

    # Initialize variables to track sample-related files and extra files
    sample1_present = False
    sample2_present = False
    extra_files = set()

    extra_file = False
    new_object_key = object_key

    # Check tar file size
    obj_size = obj['Size']
    if obj_size < 1048576:  # Less than 1MB, obvious failure, delete unconditionally
        s3.delete_object(Bucket=bucket_name, Key=object_key)
        print(f"{object_key} - Deleted: Less than 1MB")
        return sample_set

    # Download and process the tar file
    with tempfile.TemporaryDirectory() as temp_dir:  # Create temp directory
        temp_file = os.path.join(temp_dir, 'temp.tar') # Create temp file
        s3.download_file(bucket_name, object_key, temp_file) # Download tar file as temp file

        # Extract the tar file to a temporary directory
        with tarfile.open(temp_file, 'r') as tar:
            tar.extractall(path=temp_dir)

        # Find the "output" directory within the temporary directory
        output_dir = os.path.join(temp_dir, 'output')

        if not os.path.exists(output_dir):
            print(f"{object_key} - Missing 'output' directory")
            return sample_set

        # Process the files within the "output" directory
        for root, _, files in os.walk(output_dir):
            for filename in files:
                # Check if the file starts with sample_name1 or sample_name2
                if filename.startswith(sample_name1):
                    sample1_present = True
                elif filename.startswith(sample_name2):
                    sample2_present = True
                else:
                    # Extra file not starting with any sample name
                    extra_files.add(filename)  # Mismatch between sample name and expected sample names
                    extra_files = True

        # Remove extra files not starting with sample names
        for filename in extra_files:
            file_path = os.path.join(output_dir, filename)
            os.remove(file_path)

        # If not deleted or modified, check for missing sample files and take appropriate action
        if not sample1_present or not sample2_present:
            if not sample1_present and not sample2_present:   # If both samples are missing, delete (shouldn't happen because of the <1MB check)
                # Missing both sets of files
                action_message = "Deleted"
                s3.delete_object(Bucket=bucket_name, Key=object_key)
            else:  # If only one is missing
                # Missing one set of files
                action_message = "Renamed"
                # Remove the missing sample_id from the tar file name
                if sample1_present:
                    new_object_key = os.path.join(bucket_directory, f"{sample_name1}.tar")
                else:
                    new_object_key = os.path.join(bucket_directory, f"{sample_name2}.tar")

                if extra_files:
                    # if extra files upload the modified tar file with new name
                    with tempfile.NamedTemporaryFile(delete=False) as new_temp_file:  # Create temp tar file
                        with tarfile.open(new_temp_file.name, 'w') as new_tar:  # Open temp tar file to write to
                            with tarfile.open(temp_file, 'r') as tar:    # Open original tar file to read from
                                for member in tar.getmembers():
                                    filename = os.path.join(output_dir, os.path.basename(member.name))
                                    if filename.startswith(sample_name1) or filename.startswith(sample_name2):
                                        new_tar.add(filename, os.path.basename(filename))  # Write files that start with unique sample1 or sample2
                                
                                s3.upload_file(new_temp_file.name, bucket_name, new_object_key)
                else:
                    s3.copy_object(CopySource={'Bucket': bucket_name, 'Key': object_key}, Bucket=bucket_name, Key=new_object_key) 

                s3.delete_object(Bucket=bucket_name, Key=object_key)

            print(f"{object_key} - {action_message}: Missing Sample Files")
            
        else:  # If tar filename reflects the samples present, check if either are duplicates
            added = False  # Flag to tell whether or not to upload the modified tar file
            # Remove duplicates and update tar file
            if sample_name1 in sample_set and sample_name2 in sample_set:  # Delete tar file if both are duplicates
                s3.delete_object(Bucket=bucket_name, Key=object_key)
                print(f"{object_key} - Deleted: All Duplicate Samples")
            elif sample_name1 in sample_set or sample_name2 in sample_set:
                with tempfile.NamedTemporaryFile(delete=False) as new_temp_file:  # Create temp tar file
                    with tarfile.open(new_temp_file.name, 'w') as new_tar:  # Open temp tar file to write to
                        with tarfile.open(temp_file, 'r') as tar:    # Open original tar file to read from
                            # sample_name1 is not already in sample_set, add to new tar
                            if sample_name1 not in sample_set:  # If sample1 is unique
                                new_object_key = os.path.join(bucket_directory, f"{sample_name1}.tar")
                                for member in tar.getmembers():
                                    filename = os.path.join(output_dir, os.path.basename(member.name))
                                    if filename.startswith(sample_name1):
                                        new_tar.add(filename, os.path.basename(filename))  # Write files that start with unique sample1
                            # sample_name_name2 is not already in sample_set, add to new tar
                            if sample_name2 not in sample_set:  # If sample 2 is unique
                                new_object_key = os.path.join(bucket_directory, f"{sample_name2}.tar")
                                for member in tar.getmembers():
                                    filename = os.path.join(output_dir, os.path.basename(member.name))
                                    if filename.startswith(sample_name2):
                                        new_tar.add(filename, os.path.basename(filename))  # Write files that start with unique sample2

                added = True  # Set the flag to upload the modified tar file

            if added:  # Upload modified tar file with new name
                print(f"{object_key} - Modified: Duplicate Sample")
                print("new temp file 2 upload: ", new_temp_file.name, new_object_key)
                s3.upload_file(new_temp_file.name, bucket_name, new_object_key)
    
    sample_set.add(sample_name1)
    sample_set.add(sample_name2)
    return sample_set  # Update unique sample_set

def handle_1_sample_case(sample_set, samples, bucket_name, object_key, obj, s3, bucket_directory):
    sample_name = samples[0]

    # Initialize variables to track sample-related files and extra files
    sample_present = False
    extra_files = set()

    # Check tar file size
    obj_size = obj['Size']
    if obj_size < 1048576:  # Less than 1MB
        s3.delete_object(Bucket=bucket_name, Key=object_key)
        print(f"{object_key} - Deleted: Less than 1MB")
        return sample_set

    # Download and process the tar file
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file = os.path.join(temp_dir, 'temp.tar')
        s3.download_file(bucket_name, object_key, temp_file)

        # Extract the tar file to a temporary directory
        with tarfile.open(temp_file, 'r') as tar:
            tar.extractall(path=temp_dir)

        # Find the "output" directory within the temporary directory
        output_dir = os.path.join(temp_dir, 'output')

        if not os.path.exists(output_dir):
            print(f"{object_key} - Missing 'output' directory")
            return sample_set

        # Process the files within the "output" directory
        for root, _, files in os.walk(output_dir):
            for filename in files:
                # Check if the file starts with sample_name
                if filename.startswith(sample_name):
                    sample_present = True
                else:
                    # Extra file not starting with the sample name
                    extra_files.add(filename)

        # Remove extra files not starting with the sample name
        for filename in extra_files:
            file_path = os.path.join(output_dir, filename)
            os.remove(file_path)

        # If not deleted or modified, check for missing sample files and take appropriate action
        if not sample_present:
            # Missing both sets of files
            s3.delete_object(Bucket=bucket_name, Key=object_key)
            print(f"{object_key} - Deleted : Missing Sample Files")
        
        else:
            # Remove if duplicates
            if sample_name in sample_set:
                s3.delete_object(Bucket=bucket_name, Key=object_key)
                print(f"{object_key} - Deleted: Duplicate Sample")

    return sample_set

# Function to process tar files
def process_tar_files(bucket_name, bucket_directory):
    # Initialize the S3 client
    s3 = boto3.client('s3')

    # Set to track sample sets
    sample_set = set()  # store unique sample ids

    # Iterate through objects/pages in the bucket
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=bucket_directory)

    for i, objects in enumerate(pages):
        for j, obj in enumerate(objects.get('Contents', [])):
            if j > 100:
                break

            object_key = obj['Key']
            if object_key.endswith('.tar'):
                # Extract sample names from the tar filenames
                samples = os.path.basename(object_key).split(".tar")[0].split('___')

                if any([sample is None for sample in samples]):
                    print(samples)
                    print(object_key)
                    continue

                if len(samples) == 2:
                    sample_set = handle_2_sample_case(sample_set, samples, bucket_name, object_key, obj, s3, bucket_directory)
                elif len(samples) == 1:
                    split = samples[0].split("_vcpa1.1")[:-1]

                    if len(split) == 2:
                        reconstructed_name = f"{ split[0] }__{ split[1] }.tar"
                        # Name the tar with the reconstructed name
                        s3.copy_object(CopySource={'Bucket': bucket_name, 'Key': object_key}, 
                                       Bucket=bucket_name, Key=os.path.join(bucket_directory, reconstructed_name))
                        # Delete the old tar
                        s3.delete_object(Bucket=bucket_name, Key=object_key)
                    else:
                        sample_set = handle_1_sample_case(sample_set, samples, bucket_name, object_key, obj, s3, bucket_directory)
                else:
                    continue
    
    print("Finished fixing bucket... if not, hope you made a backup!")

# Example usage:
# process_tar_files('your_bucket_name', 'your_bucket_directory')
