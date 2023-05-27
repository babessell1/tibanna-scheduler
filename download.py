#!/usr/bin/python
import boto3
import os

inbucket = "s3://niagads-bucket"
locations = [
   #"s3://wanglab-dss-share/distribution/adsp/cram/snd10011/A-ROS-RS000011-BL-RUS-a0608_vcpa1.1.cram",
   "s3://wanglab-dss-share/distribution/adsp/cram/snd10011/A-ROS-RS000014-BR-RUS-a0610_vcpa1.1.cram"
]
filenames = [loc.split("/")[-1] for loc in locations]


def download_and_index():
  for loc, file in zip(locations, filenames):
    print( f"aws s3 cp --request-payer requester {loc} {inbucket}/crams/{file}" )
    os.system(f"aws s3 cp --request-payer requester {loc} {inbucket}/crams/{file}")
    os.system(f"samtools index {inbucket}/crams/{file}")
    os.system(f"aws s3 mv {inbucket}/crams/{file}.crai {inbucket}/cramsidx/{file}.crai")
    
if __name__ == "__main__":
  download_and_index()