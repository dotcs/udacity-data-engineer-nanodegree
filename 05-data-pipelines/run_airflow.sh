#!/usr/bin/env bash

export AIRFLOW_HOME=`pwd`/airflow
export AIRFLOW_PLUGINS=`pwd`/airflow/plugins
export AIRFLOW__CORE__LOAD_EXAMPLES=False

airflow initdb

airflow webserver -p 8080 &
P1=$!
airflow scheduler &
P2=$!
wait $P1 $P2
