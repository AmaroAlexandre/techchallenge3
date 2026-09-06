from src.config import GOLD_CSV_DIR
from src.ingest_bronze import ingest_bronze
from src.spark_utils import build_spark
from src.transform_gold import transform_gold
from src.transform_silver import transform_silver


def run_pipeline() -> None:
    print("1 bronze")
    ingest_bronze()
    spark = build_spark("techchallenge3")
    try:
        print("2 silver")
        transform_silver(spark)
        print("3 gold")
        transform_gold(spark)
    finally:
        spark.stop()
    print("pronto")
    print(GOLD_CSV_DIR)


if __name__ == "__main__":
    run_pipeline()
