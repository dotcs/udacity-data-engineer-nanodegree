from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

class RedditPlugin(AirflowPlugin):
    name = 'reddit_plugin'
    operators = [
        operators.S3UploadOperator,
        operators.StageToRedshiftOperator,
        operators.LoadDWHTableOperator,
        operators.DataQualityOperator,
    ]
    helpers = [
        helpers.SqlQueries,
        helpers.DataQualityQueries,
    ]