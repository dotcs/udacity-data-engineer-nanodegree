# Udacity Data Engineer Nanodegree - Project 4: Data Lake

In this project (Py)Spark is used to process large amount of data for a
fictional music streaming service, which is called Sparkify.

## Problem

The scenario that should be solved in this project is: 
Sparkify has gained a lot new users and the song database as well as the
recorded song plays have increased over time.

Sparkify has created a dump of the data in Amazon S3 storage.
This dump currently consists of JSON logs of the user activity and metadata
on the songs.

## Solution

We read the data from with (Py)Spark and transform it into a star schema.
Finally we store the results as parquet files from which they can be easily
processed further on.

## Getting started

A working Python (>= Python 3.6) environment is required.

In this enviroment run

```bash
pip install pyspark
``` 

or use the provided Anaconda environment

```bash
conda create -p ./.conda-env --file conda.txt
conda activate ./.conda-env
```

Then execute the `etl.py` script, which can either be executed in a local
mode or the default (remote) mode:

Local mode:
```bash
python etl.py local --help   # list all parameters
python etl.py local          # runs the script in local mode (with default params)
```

Remote mode:
```bash
python etl.py remote --help  # list all parameters
python etl.py remote --s3-bucket-target s3a://your-bucket-id
```

When running in the default mode, make sure to enter AWS credentials in the
`dl.cfg` file first.