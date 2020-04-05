#!/usr/bin/env python3
import subprocess
import logging
import sys
import math
import argparse

logger = logging.getLogger(__name__)

def count_rows(file):
    cmd = f'zstd -cdq {file} | wc -l'
    logger.debug(f'Run cmd: {cmd}')
    count = int(subprocess.check_output(cmd, shell=True).decode().strip())
    logger.info(f'File has {count} number of rows')
    return count


def make_chunks(file, output_path, n_rows, chunk_size=1000):
    logger.info(f'Work on file \'{file}\' and write chunks to \'{output_path}\'')
    logger.debug(f'Chunk size: {chunk_size}')
    logger.debug(f'Rows: {n_rows}')
    n_chunks = math.ceil(n_rows / chunk_size)
    logger.debug(f'Will write {n_chunks} chunks')
    for i in range(n_chunks):
        chunk_start = i * chunk_size + 1
        chunk_end = (i+1) * chunk_size
        cmd1 = f'mkdir -p {output_path}/{i}'
        cmd2 = f'zstd -cdq {file} | tail -n +{chunk_start} | head -n {chunk_size} > {output_path}/{i}/chunk.ndjson'

        subprocess.check_call(cmd1.split(' '))
        subprocess.check_call(cmd2, shell=True)

        if i % 100 = 0:
            logger.info(f'Wrote chunk {i} of {n_chunks}')


def run():
    handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    files = [
        ('input/RS_2019-08.zst', 'input/chunks/submissions'),
        ('input/Reddit_Subreddits.ndjson.zst', 'input/chunks/subreddits')
    ]

    for file, output_path in files:
        n_rows = count_rows(file)
        make_chunks(file, output_path, n_rows)


if __name__ == "__main__":
    run()
