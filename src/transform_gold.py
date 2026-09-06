from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config import GOLD_CSV_DIR, is_s3, gold_uri, silver_uri


def _read_silver(spark: SparkSession) -> DataFrame:
    return spark.read.parquet(f"{silver_uri()}/professionals")


def _write(frame: DataFrame, name: str) -> None:
    parquet_path = f"{gold_uri()}/{name}"
    frame.write.mode("overwrite").parquet(parquet_path)
    if not is_s3(parquet_path):
        GOLD_CSV_DIR.mkdir(parents=True, exist_ok=True)
        frame.toPandas().to_csv(GOLD_CSV_DIR / f"{name}.csv", index=False)
    print("gold", name, "->", parquet_path)


def transform_gold(spark: SparkSession) -> dict[str, DataFrame]:
    silver = _read_silver(spark)
    employed = silver.filter(F.col("is_employed") == 1)

    # recorte empregado entra nas contas de mercado/salario/regiao

    overview = silver.groupBy("survey_year", "edition").agg(
        F.count("*").alias("respondents"),
        F.sum(F.col("is_employed")).alias("employed"),
        F.round(F.avg(F.when(F.col("gender_group") == "Feminino", 1).otherwise(0)) * 100, 1).alias("pct_women"),
        F.round(F.avg(F.when(F.col("work_model_group") == "Remoto", 1).otherwise(0)) * 100, 1).alias("pct_remote"),
        F.round(F.avg("salary_midpoint"), 0).alias("avg_salary_midpoint"),
        F.round(F.avg("uses_python") * 100, 1).alias("pct_python"),
        F.round(F.avg("uses_sql") * 100, 1).alias("pct_sql"),
        F.round(F.avg("uses_aws") * 100, 1).alias("pct_aws"),
        F.round(F.avg("uses_genai") * 100, 1).alias("pct_genai"),
    ).orderBy("survey_year")

    market = (
        employed.groupBy("survey_year", "job_family")
        .agg(F.count("*").alias("professionals"))
        .withColumn(
            "share_pct",
            F.round(
                F.col("professionals")
                / F.sum("professionals").over(Window.partitionBy("survey_year"))
                * 100,
                1,
            ),
        )
        .orderBy("survey_year", F.desc("professionals"))
    )

    salary = (
        employed.filter(F.col("salary_midpoint").isNotNull())
        .groupBy("survey_year", "job_family", "seniority_group")
        .agg(
            F.count("*").alias("n"),
            F.round(F.avg("salary_midpoint"), 0).alias("avg_salary"),
        )
        .filter(F.col("n") >= 20)  # grupo pequeno eu corto
        .orderBy("survey_year", "job_family", "seniority_group")
    )

    gender = (
        silver.groupBy("survey_year", "gender_group", "job_family", "seniority_group")
        .agg(
            F.count("*").alias("n"),
            F.round(F.avg("salary_midpoint"), 0).alias("avg_salary"),
        )
        .orderBy("survey_year", "gender_group")
    )

    gender_gap = (
        employed.filter(F.col("gender_group").isin("Feminino", "Masculino"))
        .groupBy("survey_year", "gender_group")
        .agg(
            F.count("*").alias("n"),
            F.round(F.avg("salary_midpoint"), 0).alias("avg_salary"),
            F.round(F.avg(F.when(F.col("seniority_group") == "Sênior", 1).otherwise(0)) * 100, 1).alias("pct_senior"),
        )
    )

    tech_cols = [
        ("uses_sql", "SQL"),
        ("uses_python", "Python"),
        ("uses_aws", "AWS"),
        ("uses_azure", "Azure"),
        ("uses_gcp", "GCP"),
        ("uses_powerbi", "Power BI"),
        ("uses_tableau", "Tableau"),
        ("uses_databricks", "Databricks"),
        ("uses_snowflake", "Snowflake"),
    ]
    tech_frames = []
    answered_tech = employed.filter(F.col("uses_python").isNotNull() | F.col("uses_sql").isNotNull())
    for column, label in tech_cols:
        tech_frames.append(
            answered_tech.groupBy("survey_year")
            .agg(F.round(F.avg(F.col(column)) * 100, 1).alias("adoption_pct"), F.count("*").alias("n"))
            .withColumn("technology", F.lit(label))
        )
    tech = tech_frames[0]
    for frame in tech_frames[1:]:
        tech = tech.unionByName(frame)
    tech = tech.orderBy("survey_year", F.desc("adoption_pct"))

    region_model = (
        employed.groupBy("survey_year", "region_group", "work_model_group")
        .agg(F.count("*").alias("n"), F.round(F.avg("salary_midpoint"), 0).alias("avg_salary"))
        .orderBy("survey_year", "region_group")
    )

    ai = (
        silver.filter(F.col("uses_genai").isNotNull())
        .groupBy("survey_year", "job_family", "seniority_group")
        .agg(
            F.count("*").alias("n"),
            F.round(F.avg("uses_genai") * 100, 1).alias("pct_uses_genai"),
            F.round(F.avg("salary_midpoint"), 0).alias("avg_salary"),
        )
        .filter(F.col("n") >= 15)
    )

    sector = (
        employed.groupBy("survey_year", "sector")
        .agg(F.count("*").alias("n"), F.round(F.avg("salary_midpoint"), 0).alias("avg_salary"))
        .filter(F.col("n") >= 30)
        .orderBy("survey_year", F.desc("n"))
    )

    tables = {
        "overview": overview,
        "market_structure": market,
        "salary_role_seniority": salary,
        "gender": gender,
        "gender_gap": gender_gap,
        "tech_adoption": tech,
        "region_work_model": region_model,
        "ai_adoption": ai,
        "sector": sector,
    }
    for name, frame in tables.items():
        _write(frame, name)
    return tables


if __name__ == "__main__":
    from src.spark_utils import build_spark

    spark = build_spark("gold")
    transform_gold(spark)
    spark.stop()
