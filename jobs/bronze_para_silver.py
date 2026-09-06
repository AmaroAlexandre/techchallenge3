# Glue job 1
# CSV do bronze -> parquet do silver
# parametros: --BRONZE_S3  --SILVER_S3
# extra py files: s3://.../libs/src.zip   (o zip tem que ter a pasta src/ dentro)
# IAM: LabRole

from __future__ import annotations

import os
import sys
from pathlib import Path

try:
    from awsglue.utils import getResolvedOptions

    ARGS = getResolvedOptions(sys.argv, ["JOB_NAME", "BRONZE_S3", "SILVER_S3"])
    os.environ["BRONZE_URI"] = ARGS["BRONZE_S3"]
    os.environ["SILVER_URI"] = ARGS["SILVER_S3"]
except ImportError:
    ARGS = {"JOB_NAME": "local"}

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.spark_utils import build_spark
from src.transform_silver import transform_silver

try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext

    sc = SparkContext.getOrCreate()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(ARGS["JOB_NAME"], ARGS)
except ImportError:
    spark = build_spark("job1-silver")
    job = None

transform_silver(spark)

if job is not None:
    job.commit()
else:
    spark.stop()
