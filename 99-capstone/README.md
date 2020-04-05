# Data Engineer Nanodegree: Capstone Project

Reddit is "the frontpage of the internet" and has a broad range of
discussions about nearly every topic one can think of. This makes it a
perfect candidate for analytics.  
This project will use a publicly available
dump of Reddit and load it into a AWS Redshift warehouse so that Data
Scientists can make use of the content and for example develop a recommender
system that finds the most suitable subreddit for your purposes.

Data dumps of reddit have been made available through
[pushshift.io](http://files.pushshift.io/reddit/).
In this project I take a look into three of the available datasets:
- [Authors](http://files.pushshift.io/reddit/authors/)
- [Submissions](http://files.pushshift.io/reddit/submissions/)
- [Subreddits](http://files.pushshift.io/reddit/subreddits/)

## Initial understanding of the Dataset

### Downloading the datasets

First I download the dataset to my local machine:

```bash
bash scripts/download_datasets.sh 2019-08-01
```

The download may take a while since all datasets together are about 9 GB in
size. The download script takes care to compare the sha1 checksums of the
downloaded files where possible. 

### Getting an understanding of the datasets

Let's have a short look into the data and get a general understanding.

```
$ ls -lth input       
total 9,2G
-rw-r--r-- 1 dotcs dotcs 1,2G  3. Apr 17:00 RA_78M.csv.zst
-rw-r--r-- 1 dotcs dotcs 1,9G  3. Apr 09:08 Reddit_Subreddits.ndjson.zst
-rw-r--r-- 1 dotcs dotcs 6,1G  3. Apr 09:06 RS_2019-08.zst
```

I notice that the total size of all datasets is about 9GB - in a compressed
form.
[Zstandard](https://facebook.github.io/zstd/) is used as the compression
algorithm which is a recent development by Facebook and has been published as
Open Source Software (OSS).
To keep the impact on the disk of my local machine small I try to avoid
extracting the data, so all my local analysis will work with an on-the-fly
decompression of the data where necessary. Due to the fast decompression
Zstandard is especially suited for that, so let's try it out.

First let's get an overview of how many rows each dataset has:

```
$ zstd -cdq input/RA_78M.csv.zst | wc -l
78130495

$ zstd -cdq input/RS_2019-08.zst | wc -l
21927461

$ zstd -cdq input/Reddit_Subreddits.ndjson.zst | wc -l 
49723295
```

There are about **78M users**, **21M submissions** and information about **49M
subreddits** available.

Again note that it's not necessary to extract the data to get a first
understanding of the data.
Thanks to Unix pipes we can do extract the data on the fly and stop after we
have read several lines. Awesome!

Next let's see how the raw data is shaped:

```bash
zstd -cdq input/RA_78M.csv.zst | tail -n +45 | head -n 1 > assets/sample-author.csv
zstd -cdq input/RS_2019-08.zst | head -1 | jq > assets/sample-submission.json
zstd -cdq input/Reddit_Subreddits.ndjson.zst | head -1 | jq > assets/sample-subreddit.json
```

This gives us the two files located in the assets folder:

- [assets/sample-author.csv](./assets/sample-author.csv)
- [assets/sample-submission.json](./assets/sample-submission.json)
- [assets/sample-subreddit.json](./assets/sample-subreddit.json)

I have used those insights to infer a data model for both the staging area and
later for the tables in the Data Warehouse since the datasets did not come
with a pre-defined schema.

Sometimes it's not quite clear which values a field will contain - especially
if there are null values in the single sample that I took above.
In those cases I can take a larger sample and have a look at all values in
this field. I have used this method several times to come up with a good
data model. [jq](https://stedolan.github.io/jq/) helps to get a quick insight
into the dataset.

For example the following command

```bash
# Collect all values of the field 'subreddit' from the first 1000 rows.
# Sort them by the number of occurencies (desc).
zstd -cdq input/RS_2019-08.zst | head -n 1000 | jq ".subreddit" | sort | uniq -c | sort -nr
```

will return a result like this:

```
     31 "AskReddit"
     22 "dankmemes"
     16 "teenagers"
     15 "memes"
     13 "PewdiepieSubmissions"
     12 "MusicOL"
      9 "gonewild"
      7 "Minecraft"
      6 "no_views"
      6 "dirtypenpals"
```

Now it should be much clearer what data can be expected from the `subreddit` column.

### Data Model

Based on the insight into the data I came up with the data model that I
used to create the database tables. The following files contain the staging
and final Data Warehouse tables and thus contain the data model:

- [sql/create_staging_tables.sql](./sql/create_staging_tables.sql)
- [sq/create_dwh_tables.sql](./sql/create_dwh_tables.sql)

I kept the data model relatively close to the source data model.
Nevertheless some parts had to be changed in order to be aligned with the
Kimball Data Warehouse model concepts. For example the `fact_submission`
table has been cleaned from author and subreddit data, because this data is
available via the dimensions `dim_author` and `dim_subreddit`.

### Create samples

To work with smaller datasets for testing purposes smaller datasets can be
created. This speeds up the development cycle of the pipeline as less data
needs to be loaded.

To create a sample with 1M entries, run the following line:

```bash
./scripts/sample_dataset.sh 2019-08-01
```

This will create a sample with 1M entries for each of the datasets.
The data subsets will be stored in `input/sample_1M/`.

## ETL Pipeline with Airflow

Airflow has been used to automate all steps in the pipeline:

- Download the data to the worker
- Preprocess data (some datasets only)
- Upload the data to S3
- Extracting the data from S3 to a staging area in AWS Redshift 
- Transform and load the data to fact and dimension tables in AWS Redshift

The final pipeline looks like this:

![Reddit DAG](./assets/reddit-dag.png)

The staging tables take the data as-is. Data transformation is done while
loading the data to the fact and dimension tables. Details can be seen in the
[SQL queries](./airflow/plugins/helpers/sql_queries.py).  
For example I replace `NULL` values with the string `'default'` in the column
`fact_submission.suggested_sort`.

Because the tables `dim_author`, `fact_submission` and `dim_subreddit` all
contain timestamps, I have decided to first collect all of the timestamps,
which is done in task `load_staging_times` and then transform them and write
them to the `dim_time` table.

Data quality checks have been implemented after the staging tables have been
loaded (task: `stage_quality`) and after the Data Warehouse tables have been
filled (task: `dwh_quality`).

*Side note: The task `sample_dataset` is optional and can be left out to load the full
dataset into the AWS Redshift Data Warehouse.*

## Pitfalls

During the development of this project, I have faced several issues.

### CSV Dataset Issues

Unfortunately the CSV dataset that contains the authors is quite bad.
The data is separated by whitespace, but unfortunately the usernames can also
have a whitespace. Because loading data from S3 to Redshift can load CSV data
but cannot process data along the way I had to pre-process the data.
A small [Python script](./scripts/preprocess_authors.py) takes care of this
by reading from stdin line by line and use a Regular Expression to extract the fields.
It then outputs the same line, but this time with pipes as separators.

So for example the test entry

``` bash
# Original entry
$ cat assets/sample-author.csv           
77714 hockeyschtick 1137474000 1540040752 11329 451

# Same entry piped through preprocessing script
$ cat assets/sample-author.csv | ./scripts/preprocess_authors.py 
77714|hockeyschtick|1137474000|1540040752|11329|451
```

### Submission Dataset Issues

Also the submissions dataset had some issues. While importing the data into
the staging area in Redshift I noticed that several entries caused issues
because their text was longer than what Redshift can deal with. Redshift can
only store text up to 65k bytes.

I have isolated an [erroneous
entry](./pitfalls/submission-erroneous-entry.json) which has a too long value
in the `selftext` field.
To me this entry looks like either an error or a hacking attempt of the
Reddit platform.
Because I did not find a way to increase the size of the text field in
Redshift I decided to remove such entries, which I have done by adding
`MAXERROR as 10` to the `stage_submissions` task in Airflow.
This allows Redshift to ignore such problematic records.

## Getting started

A working Python (>= Python 3.6) environment is required.

In this enviroment run

```bash
pip install -r requirements.txt
``` 

or use the provided Anaconda environment

```bash
conda create -p ./.conda-env python=3.7 --file requirements.txt
conda activate ./.conda-env
```

Then start Airflow:

```bash
bash run_airflow.sh
```

This will start the Airflow scheduler and webserver in parallel.

Make sure to have a AWS Redshift cluster running.
The setup requires you to have two credentials stored in your Airflow
instance as connections:

| Credentials Name | Description |
|:--|:--|
| aws_credentials  | Credentials for your AWS user. Use fields `login` and `password` and set your `access_key` and `secret_access_key` here. The user must have the permissions: `AmazonS3FullAccess` and `AmazonRedshiftFullAccess`.  |
| redshift | Choose "Postgres" as the connection type. Enter your AWS Redshift credentials here. Your database name should be stored in the field named "Schema". |

![Airflow credentials management: AWS](assets/airflow-credentials-1.png)

![Airflow credentials management: Redshift](assets/airflow-credentials-2.png)

Also make sure that the tables exist on the cluster.
The SQL queries to create them can be found in the file
`create_staging_tables.sql` and `create_dwh.sql`.

Then go to https://localhost:8080 and enable the DAG `reddit` to start the
process.

## Questions

I was asked to answer a few questions that cover various Data Engineering related questions.

* **Question**: *What would happen if the data was increased by 100x?*  
  **Answer**: This would mean that the data grows from now 9GB to about 900GB (compressed). While it's technially not a problem to have a few TB of data stored on a single machine (AWS provides those kind of machines) I guess it would make sense to make use of the fact that Redshift is a distributed Data Warehouse solution and use 2 or more nodes. I expect the largest growth in the `fact_submission` table. Depending on the size of the dimension tables, and on how often they are joined, it might make sense to make them available local to all worker nodes, which can be done by setting `diststyle ALL` to the dimension tables. Also it might make sense to re-think the possible queries and optimize tables even further for the queries, e.g. by setting the `sortkey` and `distkey` values depending on the type of queries. It might even make sense to duplicate fact and/or dimension tables with a diffent sorting or distribution schema if the queries vary strongly in how the data is accessed.
* **Question**: *What would happen if the pipelines would be run on a daily basis by 7 am every day?*  
  **Answer**: Technically Airflow is perfectly suited for this scenario. Although the source datasets are aggregated by month, not by day. This means that running the pipeline on a daily basis would not make much sense - unless the data is provided in a different aggregated form.
* **Question**: *What would happen if the database needed to be accessed by 100+ people?*  
  **Answer**: The load of the database depends on the frequency and the impact of the individual queries. If the load is getting to high it would make sense to scale the database horizontally by adding more nodes to bring down the load for each individual node. Depending on the type of queries it could make sense to optimize the database in a similar way that has been described in the first question.

## Acknowledgements

I want to thank [pushshift.io](http://pushshift.io) for providing easy access
to the reddit datasets which are the base for this project. Thank you!