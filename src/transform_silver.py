from __future__ import annotations

import shutil
import unicodedata
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.columns import resolve_schema
from src.config import (
    DATASETS,
    EMPLOYED_STATUS_KEEP,
    SALARY_MIDPOINT,
    bronze_uri,
    is_s3,
    silver_uri,
)


CAMPOS = [
    "survey_year",
    "edition",
    "age",
    "age_band",
    "gender",
    "race",
    "pcd",
    "region",
    "uf",
    "education",
    "work_status",
    "sector",
    "job_title",
    "is_manager",
    "seniority",
    "salary_band",
    "experience",
    "work_model",
    "uses_python",
    "uses_sql",
    "uses_aws",
    "uses_gcp",
    "uses_azure",
    "uses_powerbi",
    "uses_tableau",
    "uses_databricks",
    "uses_snowflake",
    "job_function",
    "genai_not_used",
    "genai_free",
    "genai_paid_self",
    "genai_paid_company",
    "genai_copilot",
    "genai_use_text",
    "company_genai_priority",
]


FLAG_FIELDS = {
    field
    for field in CAMPOS
    if field not in {"genai_use_text", "company_genai_priority"}
    and (field.startswith("uses_") or field.startswith("genai_") or field == "is_manager")
}


def _input_schema() -> StructType:
    # schema na mão: o glue 5.1 não fecha o tipo sozinho
    fields = [StructField("survey_year", IntegerType(), False)]
    for field in CAMPOS:
        if field == "survey_year":
            continue
        data_type = IntegerType() if field in FLAG_FIELDS else StringType()
        fields.append(StructField(field, data_type, True))
    return StructType(fields)


def _is_missing(value) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _cell(field: str, value):
    if field == "survey_year":
        return int(value)
    if _is_missing(value):
        return None
    if field in FLAG_FIELDS:
        return int(float(value))
    return str(value)


def _rows_for_spark(frame: pd.DataFrame) -> list[tuple]:
    # pandas transforma int+nulo em 0.0 e o spark 4 do glue recusa. mando tuple.
    return [
        tuple(_cell(name, value) for name, value in zip(CAMPOS, row))
        for row in frame[CAMPOS].itertuples(index=False, name=None)
    ]


def fold(text: object) -> str:
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    value = unicodedata.normalize("NFKD", str(text).strip().lower())
    return "".join(ch for ch in value if not unicodedata.combining(ch))


def _to_flag(series: pd.Series) -> pd.Series:
    raw = series.astype("string").str.strip().str.lower()
    mapped = pd.Series(pd.NA, index=series.index, dtype="Int64")
    mapped = mapped.mask(raw.isin(["1", "1.0", "true", "sim", "yes"]), 1)
    mapped = mapped.mask(raw.isin(["0", "0.0", "false", "nao", "não", "no"]), 0)
    mapped = mapped.mask(raw.notna() & ~raw.isin(["", "nan", "<na>"]) & mapped.isna(), 1)
    return mapped


def _join_uri(base: str, *parts: str) -> str:
    chunks = [base.rstrip("/")]
    chunks.extend(str(part).strip("/") for part in parts)
    return "/".join(chunks)


def _read_csv(uri: str, local_name: str | None = None) -> pd.DataFrame:
    if is_s3(uri):
        import boto3

        parsed = urlparse(uri)
        local = Path("/tmp") / (local_name or parsed.path.rstrip("/").split("/")[-1])
        boto3.client("s3").download_file(parsed.netloc, parsed.path.lstrip("/"), str(local))
        return pd.read_csv(local, low_memory=False)
    return pd.read_csv(uri, low_memory=False)


def _read_year(survey_year: int, edition: str) -> pd.DataFrame:
    uri = _join_uri(bronze_uri(), f"survey_year={survey_year}", "survey.csv")
    frame = _read_csv(uri, local_name=f"survey_{survey_year}.csv")
    mapping = resolve_schema(frame.columns)
    out = pd.DataFrame(index=frame.index)
    out["survey_year"] = survey_year
    out["edition"] = edition
    for field in CAMPOS:
        if field in {"survey_year", "edition"}:
            continue
        source = mapping.get(field)
        if source is None:
            out[field] = pd.NA
            continue
        if field.startswith("uses_") or field.startswith("genai_") or field == "is_manager":
            if field in {"genai_use_text", "company_genai_priority"}:
                out[field] = frame[source].astype("string")
            else:
                out[field] = _to_flag(frame[source])
        else:
            out[field] = frame[source].astype("string")
    print("mapeamento", edition)
    for field, source in mapping.items():
        print(f"  {field:24} <- {source}")
    return out


def _job_family_expr():
    title = F.lower(F.coalesce(F.col("job_title"), F.lit("")))
    manager = F.coalesce(F.col("is_manager"), F.lit(0))
    return (
        F.when(title.rlike("cientista|data scientist"), "Cientista de Dados")
        .when(title.rlike("machine learning|ml engineer|ai engineer"), "Engenheiro de ML/IA")
        .when(title.rlike("engenheiro de dados|data engineer|arquiteto de dados|data architect"), "Engenharia de Dados")
        .when(title.rlike("analytics engineer"), "Analytics Engineer")
        .when(title.rlike("analista de dados|data analyst"), "Analista de Dados")
        .when(title.rlike("analista de bi|bi analyst"), "Analista de BI")
        .when(title.rlike("business analyst|analista de negocios"), "Analista de Negócios")
        .when(title.rlike("product manager|dpm"), "Data Product Manager")
        .when(title.rlike("desenvolvedor|engenheiro de software"), "Engenharia de Software")
        .when((F.trim(title) == "") & (manager == 1), "Gestão")
        .when((F.col("job_title").isNull()) | (F.trim(F.col("job_title")) == ""), "Não informado")
        .otherwise("Outros")
    )


def _work_model_expr():
    value = F.lower(F.coalesce(F.col("work_model"), F.lit("")))
    return (
        F.when(value.rlike("100% remoto|100% remote|modelo 100% remoto"), "Remoto")
        .when(value.rlike("hibrido|híbrido|hybrid"), "Híbrido")
        .when(value.rlike("presencial|office"), "Presencial")
        .when(value == "", "Não informado")
        .otherwise("Outros")
    )


def _gender_expr():
    value = F.lower(F.coalesce(F.col("gender"), F.lit("")))
    return (
        F.when(value.rlike("feminino|mulher|woman"), "Feminino")
        .when(value.rlike("masculino|homem|man"), "Masculino")
        .when(value == "", "Não informado")
        .otherwise("Outros")
    )


def _seniority_expr():
    value = F.lower(F.coalesce(F.col("seniority"), F.lit("")))
    return (
        F.when(value.rlike("junior|júnior"), "Júnior")
        .when(value.rlike("pleno"), "Pleno")
        .when(value.rlike("senior|sênior"), "Sênior")
        .when(value.rlike("gestor|gerente|head|diretor|coordenador"), "Gestão")
        .when(value == "", "Não informado")
        .otherwise(F.initcap(F.col("seniority")))
    )


def _salary_mid_expr():
    expr = F.lit(None).cast("double")
    for label, midpoint in SALARY_MIDPOINT.items():
        expr = F.when(F.lower(F.coalesce(F.col("salary_band"), F.lit(""))) == label, F.lit(float(midpoint))).otherwise(expr)
    return expr


def _employed_expr():
    status = F.lower(F.coalesce(F.col("work_status"), F.lit("")))
    expr = F.lit(0)
    for label in EMPLOYED_STATUS_KEEP:
        expr = F.when(status.contains(label), F.lit(1)).otherwise(expr)
    return expr.cast("int")


def _uses_genai_expr():
    flags = [
        F.coalesce(F.col("genai_free"), F.lit(0)),
        F.coalesce(F.col("genai_paid_self"), F.lit(0)),
        F.coalesce(F.col("genai_paid_company"), F.lit(0)),
        F.coalesce(F.col("genai_copilot"), F.lit(0)),
    ]
    any_use = flags[0]
    for flag in flags[1:]:
        any_use = any_use + flag
    text = F.lower(F.coalesce(F.col("genai_use_text"), F.lit("")))
    text_use = (~text.rlike("nao utilizo|não utilizo|^$")) & (F.length(text) > 0)
    answered = (
        F.col("genai_free").isNotNull()
        | F.col("genai_paid_self").isNotNull()
        | F.col("genai_paid_company").isNotNull()
        | F.col("genai_copilot").isNotNull()
        | F.col("genai_not_used").isNotNull()
        | F.col("genai_use_text").isNotNull()
    )
    return (
        F.when(~answered, F.lit(None).cast("int"))
        .when((any_use > 0) | text_use, F.lit(1))
        .otherwise(F.lit(0))
    )


def spark_enrich(frame: DataFrame) -> DataFrame:
    return (
        frame.withColumn("job_family", _job_family_expr())
        .withColumn("work_model_group", _work_model_expr())
        .withColumn("gender_group", _gender_expr())
        .withColumn("seniority_group", _seniority_expr())
        .withColumn("salary_midpoint", _salary_mid_expr())
        .withColumn("is_employed", _employed_expr())
        .withColumn("uses_genai", _uses_genai_expr())
        .withColumn(
            "region_group",
            F.when(F.col("region").isNull() | (F.trim(F.col("region")) == ""), "Não informado").otherwise(
                F.initcap(F.col("region"))
            ),
        )
    )


def transform_silver(spark: SparkSession) -> DataFrame:
    schema = _input_schema()
    parts = []
    for dataset in DATASETS:
        rows = _rows_for_spark(_read_year(dataset["survey_year"], dataset["edition"]))
        spark_df = spark.createDataFrame(rows, schema=schema)
        parts.append(spark_df)
    unioned = parts[0]
    for part in parts[1:]:
        unioned = unioned.unionByName(part)
    silver = spark_enrich(unioned)
    target = f"{silver_uri()}/professionals"
    if not is_s3(target):
        dest = Path(target)
        if dest.exists():
            shutil.rmtree(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
    (
        silver.write.mode("overwrite")
        .partitionBy("survey_year")
        .parquet(target)
    )
    print("silver em", target)
    return silver


if __name__ == "__main__":
    from src.spark_utils import build_spark

    spark = build_spark("silver")
    transform_silver(spark)
    spark.stop()
