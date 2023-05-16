#!/bin/bash

# install aws cli
sudo apt install unzip
sudo apt install zip
sudo apt install amazon-ec2-utils
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# config 
aws s3 cp s3://niagads-bucket/k1.txt ./k1.txt
aws s3 cp s3://niagads-bucket/k2.txt ./k2.txt
aws configure set aws_access_key_id $(cat k1.txt)
aws configure set aws_secret_access_key $(cat k2.txt)
aws configure set region us-east-1

# download reference genomes
wget http://hgdownload.soe.ucsc.edu/goldenPath/hg38/bigZips/hg38.fa.gz ./genomes/hg38.fa.gz


# check out sample(s)
aws s3 cp s3://niagads-bucket/samples2call.txt ./samples2call.txt
head -n -1 "samples2call.txt" > "checkout.txt.tmp" && mv "checkout.txt.tmp" "checkout.txt" 
tail -n +2 "./samples2call.txt" > "samples2call.txt.tmp" && mv "samples2call.txt.tmp" "samples2call.txt"
aws s3 cp ./samples2call.txt s3://niagads-bucket/samples2call.txt


aws s3 cp s3://niagads-bucket/init_instance.sh init_instance.sh
bash init_instance.sh
aws s3 cp install_log.txt s3://niagads-bucket/install.log


$(ec2-metadata -i | cut -d " " -f2)
sudo apt install amazon-ec2-utils
aws ec2 associate-iam-instance-profile --iam-instance-profile Name=ec2-role --instance-id $(ec2-metadata -i | cut -d " " -f2)


aws s3 cp --request-payer requester s3://wanglab-dss-share/distribution/adsp/cram/snd10011/A-ROS-RS000011-BL-RUS-a0608_vcpa1.1.cram A-ROS-RS000011-BL-RUS-a0608_vcpa1.1.cram