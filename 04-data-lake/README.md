# Udacity Data Engineer Nanodegree - Project 4: Data Lake

In this project AWS Redshift is used as a Data Warehouse for a fictional music streaming service, which is called Sparkify.

## Problem

The scenario that should be solved in this project is: 
Sparkify has gained a lot new users and the song database as well as the recorded song plays have increased over time. 

Sparkify has created a dump of the data in Amazon S3 storage.
This dump currently consists of JSON logs of the user activity and metadata on the songs.

## Solution

We read the data from with (Py)Spark and transform it into a star schema.
Finally we store the results as parquet files from which they can be easily processed further on.

## Getting started

tbd