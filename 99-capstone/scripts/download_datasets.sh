#!/usr/bin/env bash

set -e

USAGE="$0 <DATE>"

if [ "$#" -ne 1 ]; then
    echo $USAGE
    echo "ERROR: Wrong number of arguments."
    exit 1
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT_DIR="$SCRIPT_DIR/.."
DATA_FOLDER="$ROOT_DIR/input"

YEAR=`echo $1 | cut -d'-' -f1`
MONTH=`echo $1 | cut -d'-' -f2`

echo "Download datasets for ${YEAR}/${MONTH}"

mkdir -p $DATA_FOLDER

function download_dataset() {
    URL=$1
    FILE=`basename $URL`
    FILEPATH="$DATA_FOLDER/$FILE"
    if [ ! -e "$FILEPATH" ]; then
        echo "Download file $FILE from pushshift.io"
        curl -L -o "$FILEPATH" $URL  
    else
        echo "File $FILE found. Skip download."
    fi
}

function check_sha1sum() {
    FILE=$1
    SHA1SUM=$2
    FILEPATH="$DATA_FOLDER/$FILE"
    echo "$SHA1SUM  $FILEPATH" | sha1sum -c -
}

URL="http://files.pushshift.io/reddit/subreddits/Reddit_Subreddits.ndjson.zst" # last modified Feb 12 2019 5:56 PM
SHA1_CHECKSUM="1771bfb0296c578123ef8a17bacfb98cd4f56650"
download_dataset $URL
check_sha1sum `basename $URL` $SHA1_CHECKSUM

URL="http://files.pushshift.io/reddit/authors/RA_78M.csv.zst" # last modified Apr 23 2019 3:53 AM
SHA1_CHECKSUM="0866e773c142f8e4e094b54696ab5c711db974cb"
download_dataset $URL
check_sha1sum `basename $URL` $SHA1_CHECKSUM

URL="http://files.pushshift.io/reddit/submissions/RS_$YEAR-$MONTH.zst" # last modified Sep 11 2019 5:43 AM
download_dataset $URL $SHA1_CHECKSUM
