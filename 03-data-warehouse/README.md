# Udacity Data Engineer Nanodegree - Project 3: Create an AWS Redshift Data Warehouse

In this project AWS Redshift is used as a Data Warehouse for a fictional music streaming service, which is called Sparkify.

## Problem

The scenario that should be solved in this project is: 
Sparkify has gained a lot new users and the song database as well as the recorded song plays have increased over time. 

Sparkify has created a dump of the data in Amazon S3 storage.
This dump currently consists of JSON logs of the user activity and metadata on the songs.

## Solution

We use AWS Redshift as a data warehouse in order to make the data available to a wider audience.
The data will first be loaded from S3 into a staging area in Redshift, where we create two tables that stores the data in a simlar schema as the dumped data on S3.

In a second step the data is then transformed into a star schema which is optimized for data analytics queries.

## Getting started

Make sure to have a AWS Redshift cluster up and running and enter the login details in the `dwh.cfg` file.
The cluster must be configured, such that it can be reached from public if the scripts are not executed within the VPC.

Then start the pipeline:

1. Create the tables by running `python create_tables.py` from the terminal.
2. Run the ETL pipeline to extract the data from S3 to Redshift and transform/load it into the target tables.

Both scripts log also to stdout, so that we can see what is happening while we create tables or load data into Redshift.
The ETL step takes quite some time, so please be patient.

## Data Layout

The data layout in the Data Warehouse follows the star schema.
It consists of one fact table `fact_songplay` and four dimension tables: `dim_user`, `dim_song`, `dim_artist` and `dim_time`.

In a larger cluster I expect analytic queries to be user and song centric, which means that these tables will often be joined.
As long as these tables do not grow too much in size it makes sense to replicate them to all nodes to mitigate data transfer across nodes.
Because of this tables `dim_user` and `dim_song` have attached `diststyle ALL` in their `CREATE TABLE` statements.

Also I expect queries to be mostly interested in the latest events, which is why `fact_songplay.start_time` is the leading column for the sort key.
This means that Redshift can [skip entire blocks that fall outside a time range](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html) in queries that select data based on the time.

Since the artist table `dim_artist` is not shared with all nodes in the cluster, opposite to `dim_user` and `dim_song`, the column `artist_id` (`fact_songplay.artist_id`, `dim_artist.artist_id`) as been defined as distribution key.
Because `dim_artist.artist_id` is defined as sort key it should [enable the query optimizer to choose a sort merge join instead of a slower hash join](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html).

## Transformations

### Dimension: Artist

For demonstration purposes:
When inserting data into the `dim_artist` table, I check for `NULL` values or empty strings in the column `location`.
I don't want to have empty values in this column, so I replace the missing value with `N/A`.

### Dimension: Time

As usual various values are extracted from the timestamp, such as `hour`, `day`, `week`, etc.
