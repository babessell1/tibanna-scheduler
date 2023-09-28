import os
import tarfile
import tempfile
import boto3

def handle_2_sample_case(sample_set, samples, bucket_name, object_key, obj, s3):
    sample_name1 = samples[0]
    sample_name2 = samples[1]

    print("samples: ", sample_name1, sample_name2)

    # Initialize variables to track sample-related files and extra files
    sample1_present = False
    sample2_present = False
    extra_files = set()

    # Check tar file size
    obj_size = obj['Size']
    if obj_size < 1048576:  # Less than 1MB, obvious failure, delete unconditionally
        ###s3.delete_object(Bucket=bucket_name, Key=object_key)
        print(f"{object_key} - Deleted: Less than 1MB")
        return

    # Download and process the tar file
    with tempfile.TemporaryDirectory() as temp_dir:  # create temp directory
        temp_file = os.path.join(temp_dir, 'temp.tar') # create temp file
        s3.download_file(bucket_name, object_key, temp_file) # download tar file as temp file

        with tarfile.open(temp_file, 'r') as tar:
            for member in tar.getmembers():  # for each object in the tar file
                filename = os.path.basename(member.name)

                # Check if the file starts with sample_name1 or sample_name2
                if filename.startswith(sample_name1):
                    sample1_present = True
                elif filename.startswith(sample_name2):
                    sample2_present = True
                else:
                    # Extra file not starting with any sample name
                    extra_files.add(filename)  # mismatch between sample name and expected sample names

            # Remove extra files not starting with sample names
            if extra_files:
                with tempfile.NamedTemporaryFile(delete=False) as new_temp_file:  # create temp file to write to and upload
                    with tarfile.open(new_temp_file.name, 'w') as new_tar:  
                        with tarfile.open(temp_file, 'r') as tar: # open original tar file, already done this will this cause problems?
                            for member in tar.getmembers():  # for each object in the original tar file
                                filename = os.path.basename(member.name)
                                if filename not in extra_files:  # add files that are not mismatches
                                    new_tar.addfile(member, tar.extractfile(member))

                # Upload the modified tar file
                ###s3.upload_file(new_temp_file.name, bucket_name, object_key)

        # If not deleted or modified, check for missing sample files and take appropriate action
        if not sample1_present or not sample2_present:
            if not sample1_present and not sample2_present:   # if both samples are missing, delete (shouldnt happen because of the <1MB check)
                # Missing both sets of files
                action_message = "Deleted"
                ###s3.delete_object(Bucket=bucket_name, Key=object_key)
            else:  # if only one is missing
                # Missing one set of files
                action_message = "Renamed"
                # Remove the missing sample_id from the tar file name
                if sample1_present:
                    new_object_key = f"{sample_name1}"
                else:
                    new_object_key = f"{sample_name2}"

                # Copy the tar file to a new name and delete the original, shouldnt need to do anything else since extra files are already removed
                ###s3.copy_object(CopySource={'Bucket': bucket_name, 'Key': object_key}, Bucket=bucket_name, Key=new_object_key) 
                ###s3.delete_object(Bucket=bucket_name, Key=object_key)

            print(f"{object_key} - {action_message}: Missing Sample Files")
            
        else:  # if tar filename reflects the samples present, check if either are duplicates
            added = False  # flag to tell whether or not to upload the modified tar file
            # Remove duplicates and update tar file
            if sample_name1 in sample_set and sample_name2 in sample_set:  # delete tar file if both are duplicates
                ###s3.delete_object(Bucket=bucket_name, Key=object_key)
                print(f"{object_key} - Deleted: All Duplicate Samples")
            elif sample_name1 in sample_set or sample_name2 in sample_set:
                with tempfile.NamedTemporaryFile(delete=False) as new_temp_file:  # create temp tar file
                    with tarfile.open(new_temp_file.name, 'w') as new_tar:  # open temp tar file to write to
                        with tarfile.open(temp_file, 'r') as tar:    # open original tar file to read from
                            # sample_name1 is not already in sample_set, add to new tar
                            if sample_name1 not in sample_set:  # if sample1 is unique
                                for member in tar.getmembers():
                                    filename = os.path.basename(member.name)
                                    if filename.startswith(sample_name1):
                                        new_tar.addfile(member, tar.extractfile(member))  # write files that start with unique sample1
                            # sample_name_name2 is not already in sample_set, add to new tar
                            if sample_name2 not in sample_set:  # if sample 2 is unique
                                for member in tar.getmembers():
                                    filename = os.path.basename(member.name)
                                    if filename.startswith(sample_name2):
                                        new_tar.addfile(member, tar.extractfile(member)) # write files that start with unique sample2

            if added:  # upload modified tar file
                print(f"{object_key} - Modified: Duplicate Sample")
                ###s3.upload_file(new_temp_file.name, bucket_name, object_key)

    sample_set.add(sample_name1)
    sample_set.add(sample_name2)
    return sample_set  # update unique sample_set

def handle_1_sample_case(sample_set, samples, bucket_name, object_key, obj, s3):
    sample_name = samples[0]

    print("sample: ", sample_name)

    # Initialize variables to track sample-related files and extra files
    sample_present = False
    extra_files = set()

    # Check tar file size
    obj_size = obj['Size']
    if obj_size < 1048576:  # Less than 1MB
        ###s3.delete_object(Bucket=bucket_name, Key=object_key)
        print(f"{object_key} - Deleted: Less than 1MB")
        return

    # Download and process the tar file
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file = os.path.join(temp_dir, 'temp.tar')
        s3.download_file(bucket_name, object_key, temp_file)

        with tarfile.open(temp_file, 'r') as tar:
            for member in tar.getmembers():
                filename = os.path.basename(member.name)

                # Check if the file starts with sample_name1 or sample_name2
                if filename.startswith(sample_name):
                    sample_present = True
                else:
                    # Extra file not starting with any sample name
                    extra_files.add(filename)

            # Remove extra files not starting with sample names
            if extra_files:
                with tempfile.NamedTemporaryFile(delete=False) as new_temp_file:
                    with tarfile.open(new_temp_file.name, 'w') as new_tar:
                        with tarfile.open(temp_file, 'r') as tar:
                            for member in tar.getmembers():
                                filename = os.path.basename(member.name)
                                if filename not in extra_files:
                                    new_tar.addfile(member, tar.extractfile(member))

                # Upload the modified tar file
                ###s3.upload_file(new_temp_file.name, bucket_name, object_key)

        # If not deleted or modified, check for missing sample files and take appropriate action
        if not sample_present:
            # Missing both sets of files
            action_message = "Deleted"
            ###s3.delete_object(Bucket=bucket_name, Key=object_key)
            print(f"{object_key} - {action_message}: Missing Sample Files")
        
        else:
            # remove if duplicates
            if sample_name in sample_set:
                ###s3.delete_object(Bucket=bucket_name, Key=object_key)
                print(f"{object_key} - Deleted: Duplicate Sample")

# Function to process tar files
def process_tar_files(s3, bucket_name, bucket_directory):
    # Set to track sample sets
    sample_set = set()  # store unique sample ids

    # List objects in the S3 bucket
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=bucket_directory)

    for i,obj in enumerate(objects.get('Contents', [])):
        object_key = obj['Key']
        if object_key.endswith('.tar'):
            if i > 10:
                break

            # Extract sample names from the tar filenams
            samples = os.path.basename(object_key).split('___')
            if len(samples) == 2:
                sample_set = handle_2_sample_case(sample_set, samples, bucket_name, object_key, obj, s3)
            elif len(samples == 1):
                sample_set = handle_1_sample_case(sample_set, samples, bucket_name, object_key, obj, s3)
            else:
                continue
                
# Initialize the S3 client
s3 = boto3.client('s3')

# Replace with your S3 bucket name
bucket_name = 'strling-backup'
bucket_directory = "//mnt/data1/out/"
            
# Process tar files in the specified S3 bucket
process_tar_files(bucket_name, bucket_name, bucket_directory)