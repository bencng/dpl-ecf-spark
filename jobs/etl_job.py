"""
etl_job.py
~~~~~~~~~~

This Python module contains an example Apache Spark ETL job definition
that implements best practices for production ETL jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

where packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

Our chosen approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform and Load parts - into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment (e.g. a Jupyter or
Zeppelin notebook).
"""

import os

from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit

from dependencies.spark import start_spark


def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='dpl_ecf',
        files=['configs/etl_config.json'])

    # log that main ETL job is starting
    log.warn('dpl_ecf is up-and-running')

    # execute ETL pipeline
    data_frames = extract_data(spark, config)
    data_frames_drop, data_frames_transpose = transform_data(data_frames, config)
    load_data(data_frames_drop, data_frames_transpose, config)

    # log the success and terminate Spark application
    log.warn('dpl_ecf is finished')
    spark.stop()
    return None


def extract_data(spark, config):
    """Load data from Parquet file format.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    data_frames = {
        "main_file": spark.read.load(config['path_data'] + os.path.sep + 'main-file.csv', format="csv", inferSchema="true", header="true")
    }

    return data_frames


def transform_data(data_frames, config):
    """Transform original dataset.

    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """
    # df_transformed = (
    #     df
    #     .select(
    #         col('id'),
    #         concat_ws(
    #             ' ',
    #             col('first_name'),
    #             col('second_name')).alias('name'),
    #            (col('floor') * lit(steps_per_floor_)).alias('steps_to_desk')))
    #
    # return df_transformed

    # FIXME Return the data frames DROP and records to be transposed.
    return None, None


def load_data(data_frames_drop, data_frames_transpose, config):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :return: None
    """
    # (df
    #  .coalesce(1)
    #  .write
    #  .csv('loaded_data', mode='overwrite', header=True))
    return None

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
