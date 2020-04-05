#!/usr/bin/env bash

USAGE="$0 <S3_BUCKET_NAME>"

S3_BUCKET_NAME=$1

if [ -z $S3_BUCKET_NAME ]; then
    echo $USAGE
    echo "ERROR: S3 bucket name is missing."
    exit 1
fi

if ! [[ -e "input/Reddit_Subreddits.ndjson.zst" && -e "input/RS_2019-08.zst" ]]; then
    echo $USAGE
    echo "ERROR: Cannot find all files inteded to upload to S3. \
Make sure to download them first."
    exit 1
fi

echo "Upload datasets to S3"
aws s3 cp input/Reddit_Subreddits.ndjson.zst s3://$S3_BUCKET_NAME --cli-connect-timeout 6000
aws s3 cp input/RS_2019-08.zst s3://$S3_BUCKET_NAME --cli-connect-timeout 6000
aws s3 cp input/RA_78M.csv.zst s3://$S3_BUCKET_NAME --cli-connect-timeout 6000