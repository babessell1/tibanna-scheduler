mkdir -p checkout
split -d -l 1  samples2call.txt  checkout/ --additional-suffix=.txt --suffix-length=1 --numeric-suffixes=1
cp -r checkout incomplete
mkdir complete
aws s3 cp --recursive checkout s3://niagads-bucket/checkout
