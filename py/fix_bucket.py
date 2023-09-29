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

    # Check tar file size
    obj_size = obj['Size']
    if obj_size < 20000:  # Less than 20kb, obvious failure, delete unconditionally
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

        remove_extra_files = False
        # Process the files within the "output" directory
        for root, _, files in os.walk(output_dir):
            #print(f"walk, looking for: {sample_name1} and {sample_name2}")
            for filename in files:
                #print(f"f: {filename}")
                # Check if the file starts with sample_name1 or sample_name2
                if os.path.basename(filename).startswith(sample_name1):
                    sample1_present = True
                elif os.path.basename(filename).startswith(sample_name2):
                    sample2_present = True
                else:
                    # Extra file not starting with any sample name
                    extra_file_path = os.path.join(root, filename)
                    os.remove(extra_file_path)  # Remove the extra file
                    print("removed extra file: ", extra_file_path)
                    remove_extra_files = True

        # If not deleted or modified, check for missing sample files and take appropriate action
        if not sample1_present or not sample2_present:
            if not sample1_present and not sample2_present:
                # Missing both sets of files, delete (shouldn't happen because of the <1MB check)
                action_message = "Deleted"
                s3.delete_object(Bucket=bucket_name, Key=object_key)
                extra_message = ""
            else:
                # Missing one set of files, rename the tar file
                if sample1_present:
                    new_object_key = os.path.join(bucket_directory, f"{sample_name1}.tar")
                else:
                    new_object_key = os.path.join(bucket_directory, f"{sample_name2}.tar")
                
                action_message = f"Renamed to {new_object_key}"

                if remove_extra_files:

                    # Create a new tar file with the correct structure
                    with tempfile.NamedTemporaryFile(delete=False) as new_temp_file:
                        with tarfile.open(new_temp_file.name, 'w') as new_tar:
                            with tarfile.open(temp_file, 'r') as tar:
                                for member in tar.getmembers():
                                    filename = os.path.join(output_dir, os.path.basename(member.name))
                                    if os.path.basename(filename).startswith(sample_name1) or os.path.basename(filename).startswith(sample_name2):
                                        new_tar.add(filename, arcname=os.path.join("output", os.path.basename(filename)))
                                        print(f"adding {filename} to new tar")

                            s3.upload_file(new_temp_file.name, bucket_name, new_object_key)
                    extra_message = "also removed extra files"
                else: # simply rename the tar file in S3
                    extra_message = "no extra files removed"
                    s3.copy_object(CopySource={'Bucket': bucket_name, 'Key': object_key}, 
                                   Bucket=bucket_name, Key=new_object_key)
                    
                    
                # Delete the original tar file
                s3.delete_object(Bucket=bucket_name, Key=object_key)
            print(f"{object_key} - {action_message}: Missing Sample Files", extra_message )
            
        else:
            # If tar filename reflects the samples present, check if either are duplicates
            if sample_name1 in sample_set and sample_name2 in sample_set:
                # Delete tar file if both are duplicates
                s3.delete_object(Bucket=bucket_name, Key=object_key)
                print(f"{object_key} - Deleted: All Duplicate Samples")
            elif sample_name1 in sample_set or sample_name2 in sample_set:
                # Create a new tar file with the correct structure
                added = False
                with tempfile.NamedTemporaryFile(delete=False) as new_temp_file:
                    with tarfile.open(new_temp_file.name, 'w') as new_tar:
                        with tarfile.open(temp_file, 'r') as tar:
                            print(f"Looking for duplicate sample ids: {sample_name1} and {sample_name2} in {object_key}")
                            if sample_name1 not in sample_set:
                                new_object_key = os.path.join(bucket_directory, f"{sample_name1}.tar")
                                for member in tar.getmembers():
                                    print(f"member: {member.name}")
                                    filename = os.path.join(output_dir, os.path.basename(member.name))
                                    if os.path.basename(filename).startswith(sample_name1):
                                        new_tar.add(filename, arcname=os.path.join("output", os.path.basename(filename)))
                                        added = True
                                        print(f"adding {filename} to new tar")

                            elif sample_name2 not in sample_set:
                                new_object_key = os.path.join(bucket_directory, f"{sample_name2}.tar")
                                for member in tar.getmembers():
                                    print(f"member: {member.name}")
                                    filename = os.path.join(output_dir, os.path.basename(member.name))
                                    if os.path.basename(filename).startswith(sample_name2):
                                        new_tar.add(filename, arcname=os.path.join("output", os.path.basename(filename)))
                                        added = True
                                        print(f"adding {filename} to new tar")
                            else:
                                print("This should never happen")


                        if added:
                            print(f"{object_key} - Modified: Duplicate Sample")
                            print("New temp file to upload: ", new_temp_file.name, new_object_key)
                            s3.upload_file(new_temp_file.name, bucket_name, new_object_key)
                        else:
                            print(f"file name {object_key} had duplicate sample ids but no relevant files found")
                        
                # Delete the original tar file
                s3.delete_object(Bucket=bucket_name, Key=object_key)

            else: # no duplicates, no missing files, only reupload if there are extra files
                if remove_extra_files:
                    # Create a new tar file with the correct structure
                    with tempfile.NamedTemporaryFile(delete=False) as new_temp_file:
                        with tarfile.open(new_temp_file.name, 'w') as new_tar:
                            with tarfile.open(temp_file, 'r') as tar:
                                for member in tar.getmembers():
                                    filename = os.path.join(output_dir, os.path.basename(member.name))
                                    if os.path.basename(filename).startswith(sample_name1) or os.path.basename(filename).startswith(sample_name2):
                                        new_tar.add(filename, arcname=os.path.join("output", os.path.basename(filename)))
                                        print(f"adding {filename} to new tar")

                            s3.upload_file(new_temp_file.name, bucket_name, new_object_key)

                          
                    # Delete the original tar file
                    s3.delete_object(Bucket=bucket_name, Key=object_key)
                    print(f"{object_key} - Correct samples but extra files found. Your pipeline is outputing unnecessary files")

    
    sample_set.add(sample_name1)
    sample_set.add(sample_name2)
    return sample_set  # Update unique sample_set

def handle_1_sample_case(sample_set, samples, bucket_name, object_key, obj, s3, bucket_directory):
    sample_name = samples[0]

    # Initialize variables to track sample-related files and extra files
    sample_present = False

    # Check tar file size
    obj_size = obj['Size']
    if obj_size < 20000:  # Less than 20kb, obvious failure, delete unconditionally
        s3.delete_object(Bucket=bucket_name, Key=object_key)
        print(f"{object_key} - Deleted: Less than 20KB")
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
                if os.path.basename(filename).startswith(sample_name):
                    sample_present = True
                else:
                    # Extra file not starting with the sample name
                    extra_file_path = os.path.join(root, filename)
                    os.remove(extra_file_path)

        # If not deleted or modified
        if not sample_present:
            # Missing both sets of files, delete (shouldn't happen because of the <1MB check)
            s3.delete_object(Bucket=bucket_name, Key=object_key)
            print(f"{object_key} - Deleted : Missing Sample Files")
        
        else:
            # If tar filename reflects the samples present, check if it's a duplicate
            if sample_name in sample_set:
                # Delete tar file if it's a duplicate
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
            # only use between 200 and 300
            object_key = obj['Key']
            if object_key.endswith('.tar'):
                # Extract sample names from the tar filenames
                samples = os.path.basename(object_key).split(".tar")[0].split('___')

                if any([sample is None for sample in samples]):
                    print("None sample found")
                    print(samples)
                    print(object_key)
                    continue

                if len(samples) == 2:
                    try:
                        sample_set = handle_2_sample_case(sample_set, samples, bucket_name, object_key, obj, s3, bucket_directory)
                    except:
                        print(f"Error handling 2 {object_key}. Deleting tar file:")
                        s3.delete_object(Bucket=bucket_name, Key=object_key)
                        continue
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
                        try:
                            sample_set = handle_1_sample_case(sample_set, samples, bucket_name, object_key, obj, s3, bucket_directory)
                        except:
                            print(f"Error handling 1 {object_key}. Deleting tar file:")
                            s3.delete_object(Bucket=bucket_name, Key=object_key)
                            continue
                else:
                    continue
    
    print("Finished fixing bucket... if not, hope you made a backup!")

# Example usage:
# process_tar_files('your_bucket_name', 'your_bucket_directory')
