# ETL Pipeline for Song Data in Sparkify

This repostitory contains an ETL pipeline to extract data from JSON files and
populate a PostgreSQL database.

It creates one fact table and many dimension tables in a snowflake schema.

## Prerequisits

A PostgreSQL database must be running on localhost and listening to the default
port `5432`. This is already fulfilled in the workspace that Udacity provides.
For local development please refer to the section "Local development" in 
this document.

Data that should be extracted must live in the `data/` folder.

## Overview

```
.
├── create_tables.py       Script to create database and tables
├── data/                  Folder that contains the raw data (e.g. logs)
├── docker-compose.yaml    Docker environment (local development only)
├── etl.ipynb              Jupyter Notebook that explains the ETL process
├── etl.py                 ETL script based on the Jupyter Notebook
├── README.md              This file
├── requirements.txt       Python environment (local development only)
├── sql_queries.py         Contains all raw SQL queries used in other files
└── test.ipynb             Jupyter Notebook that can be used for ad-hoc db queries
```

## Start ETL Pipeline

To start the ETL pipeline first create the tables (if necessary) with

```bash
python create_tables.py
```

The ETL script can then be started using the `etl.py` file:

```bash
python etl.py
```

## Development

The Jupyter Notebooks can be used for developing. `etl.ipynb` mirrors what happens
in `etl.py` and contains most of the business logic.
`test.ipynb` can be used to make ad-hoc queries against the database.

## Local development

These steps are necessary for local development in case the default workspace
that is provided by Udacity cannot be used.

### Database setup

The database setup requires docker and docker-compose to be installed. 
To spin-up a pre-configured database that can be used for local development, use

```bash
docker-compose up -d
```

The PostgreSQL instance listens on port `5432`, while a pgAdmin instance is
running on port `80`.

### Python setup

The Python setup requires Anaconda to be installed.
To setup the Anaconda environment use:

```bash
conda create -p ./.env python=3.7
conda activate ./.env
conda install --file requirements.txt
```

Then follow the steps in section "Start ETL Pipeline".

pgAdmin or the notebook `test.ipynb` can be used to make ad-hoc queries.
