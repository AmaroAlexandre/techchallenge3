# Glue job 2: agrega o silver e grava as tabelas gold
# parametros: --SILVER_S3  --GOLD_S3
# mesmo src.zip do job 1

from __future__ import annotations

import os
import sys
from pathlib import Path

try:
    from awsglue.utils import getResolvedOptions

    ARGS = getResolvedOptions(sys.argv, ["JOB_NAME", "SILVER_S3", "GOLD_S3"])
    os.environ["SILVER_URI"] = ARGS["SILVER_S3"]
    os.environ["GOLD_URI"] = ARGS["GOLD_S3"]
except ImportError:
    ARGS = {"JOB_NAME": "local"}

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.spark_utils import build_spark
from src.transform_gold import transform_gold

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
    spark = build_spark("job2-gold")
    job = None

transform_gold(spark)

if job is not None:
    job.commit()
else:
    spark.stop()
