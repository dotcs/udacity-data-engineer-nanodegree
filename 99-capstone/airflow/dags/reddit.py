from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import (
    S3UploadOperator,
    StageToRedshiftOperator,
    LoadDWHTableOperator,
    DataQualityOperator,
)
from helpers import SqlQueries, DataQualityQueries

# If multiple Airflow workers are used, this folder must be located on a
# network shared disk.
BASH_SCRIPT_DIR=os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'scripts'))
DATASET_DIR=os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'input'))
S3_BUCKET_NAME="ufm8fe-udacity-capstone"

default_args = dict(
    owner='dotcs',
    start_date=datetime(2019, 8, 1),
    end_date=datetime(2019, 8, 31),
    depends_on_past=False
)

dag = DAG(
    'reddit',
    default_args=default_args,
    description='Load reddit data into S3 and Redshift',
    schedule_interval='@monthly'
)

start_task = DummyOperator(dag=dag, task_id='start_execution')

download_task = BashOperator(
    dag=dag,
    task_id='download_data',
    bash_command=BASH_SCRIPT_DIR + "/download_datasets.sh {{ ds }} "
)

preprocess_author_task = BashOperator(
    dag=dag,
    task_id='preprocess_author',
    bash_command=f'zstd -cdq {DATASET_DIR}/RA_78M.csv.zst | {BASH_SCRIPT_DIR}/preprocess_authors.py | zstd -zqfo {DATASET_DIR}/RA_78M_preprocessed.csv.zst',
)

sample_dataset_task = BashOperator(
    dag=dag,
    task_id='sample_dataset',
    bash_command=BASH_SCRIPT_DIR + "/sample_dataset.sh {{ds}} ",
)

upload_s3_task = S3UploadOperator(
    dag=dag, 
    task_id='upload_s3_data',
    execution_timeout=timedelta(hours=1),

    aws_credentials_id='aws_credentials',

    dataset_dir=DATASET_DIR,
    file_glob="**/*.zst",
    bucket_name=S3_BUCKET_NAME,
)

stage_author_task = StageToRedshiftOperator(
    dag=dag,
    task_id='stage_authors',
    execution_timeout=timedelta(hours=1),

    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',

    table='staging_authors',
    s3_src_bucket=S3_BUCKET_NAME,
    s3_src_pattern='sample_2M/RA_78M_preprocessed.csv.zst',
    data_format='csv',
    delimiter='|',
    jsonpaths='auto',
    copy_opts='ZSTD',
)
stage_subreddits_task = StageToRedshiftOperator(
    dag=dag,
    task_id='stage_subreddits',
    execution_timeout=timedelta(hours=1),
    
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',

    table='staging_subreddits',
    s3_src_bucket=S3_BUCKET_NAME,
    s3_src_pattern='sample_2M/Reddit_Subreddits.ndjson.zst',
    data_format='json',
    jsonpaths='auto',
    copy_opts='ZSTD',
)
stage_submissions_task = StageToRedshiftOperator(
    dag=dag,
    task_id='stage_submissions',
    execution_timeout=timedelta(hours=1),
    
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',

    table='staging_submissions',
    s3_src_bucket=S3_BUCKET_NAME,
    s3_src_pattern='sample_2M/RS_2019-08.zst',
    data_format='json',
    jsonpaths='auto',
    copy_opts='ZSTD\nMAXERROR as 100',
)
stage_quality_task = DataQualityOperator(
    dag=dag,
    task_id='stage_quality',

    redshift_conn_id='redshift',

    sql_queries=[
        DataQualityQueries.table_not_empty('staging_authors'),
        DataQualityQueries.table_not_empty('staging_subreddits'),
        DataQualityQueries.table_not_empty('staging_submissions'),
    ],
    test_results=[
        DataQualityQueries.table_not_empty_test,
        DataQualityQueries.table_not_empty_test,
        DataQualityQueries.table_not_empty_test,
    ],
)

dim_author_task = LoadDWHTableOperator(
    dag=dag,
    task_id='load_dim_author',

    redshift_conn_id='redshift',

    table="dim_author",
    sql_stmt=SqlQueries.dim_author_insert,
    update_mode='overwrite',
)
dim_subreddit_task = LoadDWHTableOperator(
    dag=dag,
    task_id='load_dim_subreddit',
    
    redshift_conn_id='redshift',

    table="dim_subreddit",
    sql_stmt=SqlQueries.dim_subreddit_insert,
    update_mode='overwrite',
)
fact_submission_task = LoadDWHTableOperator(
    dag=dag,
    task_id='load_fact_submission',

    redshift_conn_id='redshift',

    table="fact_submission",
    sql_stmt=SqlQueries.fact_submission_insert,
    update_mode='append',
)
stage_time_task = LoadDWHTableOperator(
    dag=dag,
    task_id='load_staging_times',

    redshift_conn_id='redshift',

    table="staging_times",
    sql_stmt=SqlQueries.staging_times_insert,
    update_mode='overwrite',
)
dim_time_task = LoadDWHTableOperator(
    dag=dag,
    task_id='load_dim_time',
    
    redshift_conn_id='redshift',

    table="dim_time",
    sql_stmt=SqlQueries.dim_time_insert,
    update_mode='overwrite',
)
dwh_quality_task = DataQualityOperator(
    dag=dag,
    task_id='dwh_quality',

    redshift_conn_id='redshift',

    sql_queries=[
        DataQualityQueries.table_not_empty('dim_author'),
        DataQualityQueries.table_not_empty('dim_subreddit'),
        DataQualityQueries.table_not_empty('dim_time'),
        DataQualityQueries.table_not_empty('fact_submission'),

        DataQualityQueries.col_does_not_contain_null('dim_author', 'author_id'),
        DataQualityQueries.col_does_not_contain_null('dim_subreddit', 'subreddit_id'),
        DataQualityQueries.col_does_not_contain_null('dim_time', 'start_time'),
        DataQualityQueries.col_does_not_contain_null('fact_submission', 'submission_id'),

        DataQualityQueries.col_does_not_contain_null('fact_submission', 'event_is_live'),
        DataQualityQueries.col_does_not_contain_null('fact_submission', 'suggested_sort'),
        DataQualityQueries.col_does_not_contain_null('fact_submission', 'whitelist_status'),

        DataQualityQueries.col_does_not_contain_str('dim_author', 'author_id', ' '),
    ],
    test_results=[
        DataQualityQueries.table_not_empty_test,
        DataQualityQueries.table_not_empty_test,
        DataQualityQueries.table_not_empty_test,
        DataQualityQueries.table_not_empty_test,

        DataQualityQueries.col_does_not_contain_null_test,
        DataQualityQueries.col_does_not_contain_null_test,
        DataQualityQueries.col_does_not_contain_null_test,
        DataQualityQueries.col_does_not_contain_null_test,

        DataQualityQueries.col_does_not_contain_null_test,
        DataQualityQueries.col_does_not_contain_null_test,
        DataQualityQueries.col_does_not_contain_null_test,

        DataQualityQueries.col_does_not_contain_str_test,
    ],
)

end_operator = DummyOperator(dag=dag, task_id='end_execution')

start_task \
    >> download_task \
    >> preprocess_author_task \
    >> sample_dataset_task \
    >> upload_s3_task \
    >> [ stage_author_task, stage_subreddits_task, stage_submissions_task ] \
    >> stage_quality_task \
    >> [ dim_author_task, dim_subreddit_task, fact_submission_task ] \
    >> stage_time_task \
    >> dim_time_task \
    >> dwh_quality_task \
    >> end_operator
