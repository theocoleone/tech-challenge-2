"""SparkSession configurada para acessar o S3 via s3a://."""

import os

from pyspark.sql import SparkSession

# Deve casar com o Hadoop embutido no PySpark 4.0.3
HADOOP_AWS_VERSION = "3.4.1"
AWS_PROFILE = "fiap-tech-challenge"
AWS_REGION = "us-east-1"


def get_spark(app_name="tech-challenge-2"):
    os.environ["AWS_PROFILE"] = AWS_PROFILE
    os.environ["AWS_REGION"] = AWS_REGION

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.jars.packages", f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
