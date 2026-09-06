import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LAKE_DIR = DATA_DIR / "lake"
REPORTS_DIR = ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"
GOLD_CSV_DIR = REPORTS_DIR / "gold"


def lake_uri() -> str:
    return os.environ.get("LAKE_URI", str(LAKE_DIR)).rstrip("/")


def bronze_uri() -> str:
    return os.environ.get("BRONZE_URI", f"{lake_uri()}/bronze")


def silver_uri() -> str:
    return os.environ.get("SILVER_URI", f"{lake_uri()}/silver")


def gold_uri() -> str:
    return os.environ.get("GOLD_URI", f"{lake_uri()}/gold")


def is_s3(uri: str) -> bool:
    return uri.startswith("s3://")


BRONZE_DIR = Path(bronze_uri()) if not is_s3(bronze_uri()) else Path("/tmp/bronze")
SILVER_DIR = Path(silver_uri()) if not is_s3(silver_uri()) else Path("/tmp/silver")
GOLD_DIR = Path(gold_uri()) if not is_s3(gold_uri()) else Path("/tmp/gold")

# survey_year = ano da coleta. a edição 2025-2026 foi a campo no fim de 2025.
DATASETS = [
    {
        "survey_year": 2023,
        "edition": "2023-2024",
        "kaggle": "datahackers/state-of-data-brazil-2023",
    },
    {
        "survey_year": 2024,
        "edition": "2024-2025",
        "kaggle": "datahackers/state-of-data-brazil-20242025",
    },
    {
        "survey_year": 2025,
        "edition": "2025-2026",
        "kaggle": "datahackers/state-of-data-brazil-2025-2026",
    },
]

SALARY_MIDPOINT = {
    "menos de r$ 1.000/mês": 500,
    "de r$ 1.001/mês a r$ 2.000/mês": 1500,
    "de r$ 2.001/mês a r$ 3.000/mês": 2500,
    "de r$ 3.001/mês a r$ 4.000/mês": 3500,
    "de r$ 4.001/mês a r$ 6.000/mês": 5000,
    "de r$ 6.001/mês a r$ 8.000/mês": 7000,
    "de r$ 8.001/mês a r$ 12.000/mês": 10000,
    "de r$ 12.001/mês a r$ 16.000/mês": 14000,
    "de r$ 16.001/mês a r$ 20.000/mês": 18000,
    "de r$ 20.001/mês a r$ 25.000/mês": 22500,
    "de r$ 25.001/mês a r$ 30.000/mês": 27500,
    "de r$ 30.001/mês a r$ 40.000/mês": 35000,
    "acima de r$ 40.001/mês": 45000,
}

EMPLOYED_STATUS_KEEP = (
    "empregado (clt)",
    "empreendedor ou empregado (cnpj)",
    "servidor público",
    "servidor publico",
    "freelancer",
    "vivo no brasil e trabalho remoto para empresa de fora do brasil",
    "vivo fora do brasil e trabalho para empresa de fora do brasil",
    "trabalho na área acadêmica/pesquisador",
    "trabalho na area academica/pesquisador",
)
