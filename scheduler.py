import boto3
import os

mybucket = "s3://niagads-bucket/"

locations =
[
  "s3://wanglab-dss-share/distribution/adsp/cram/snd10011/A-ROS-RS000011-BL-RUS-a0608_vcpa1.1.cram crams/A-ROS-RS000011-BL-RUS-a0608_vcpa1.1.cram"
]
filenames = [loc.split("/")[0] for loc in locations]

def download_and_index():
    for loc, file in zip(locations, filenames):
        os.system(f"aws s3 cp --request-payer requester {loc} {mybucket}/crams/{file}")
        os.system(f"samtools index {file}")


if __name__ == "__main__":
    download_and_index()