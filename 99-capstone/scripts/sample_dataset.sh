#!/usr/bin/env bash

set -e

USAGE="$0 <DATE>"

if [ "$#" -ne 1 ]; then
    echo $USAGE
    echo "ERROR: Wrong number of arguments."
    exit 1
fi

YEAR=`echo $1 | cut -d'-' -f1`
MONTH=`echo $1 | cut -d'-' -f2`

echo "Sample datasets for ${YEAR}/${MONTH}"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT_DIR="$SCRIPT_DIR/.."
DATA_FOLDER="$ROOT_DIR/input"

N_SAMPLES=2000000
SAMPLE_DIR=$DATA_FOLDER/sample_2M
mkdir -p $SAMPLE_DIR

zstd -cdq $DATA_FOLDER/Reddit_Subreddits.ndjson.zst | head -n $N_SAMPLES | zstd -zqfo $SAMPLE_DIR/Reddit_Subreddits.ndjson.zst
zstd -cdq $DATA_FOLDER/RA_78M.csv.zst | head -n $N_SAMPLES | zstd -zqfo $SAMPLE_DIR/RA_78M.csv.zst
zstd -cdq $DATA_FOLDER/RA_78M_preprocessed.csv.zst | head -n $N_SAMPLES | zstd -zqfo $SAMPLE_DIR/RA_78M_preprocessed.csv.zst
zstd -cdq $DATA_FOLDER/RS_2019-08.zst | head -n $N_SAMPLES | zstd -zqfo $SAMPLE_DIR/RS_$YEAR-$MONTH.zst
