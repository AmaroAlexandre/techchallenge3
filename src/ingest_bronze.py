import shutil
from pathlib import Path

import kagglehub

from src.config import BRONZE_DIR, DATASETS


def _pick_csv(root: Path) -> Path:
    csvs = sorted(root.rglob("*.csv"), key=lambda item: item.stat().st_size, reverse=True)
    if not csvs:
        raise FileNotFoundError(f"nenhum csv em {root}")
    return csvs[0]


def ingest_bronze() -> list[Path]:
    # pega o csv grande de cada pesquisa no kaggle e joga no bronze
    written: list[Path] = []
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    keep = {f"survey_year={dataset['survey_year']}" for dataset in DATASETS}
    for child in BRONZE_DIR.iterdir():
        if child.is_dir() and child.name.startswith("survey_year=") and child.name not in keep:
            shutil.rmtree(child)
            print("tirei edição antiga:", child)
    for dataset in DATASETS:
        cache_dir = Path(kagglehub.dataset_download(dataset["kaggle"]))
        source = _pick_csv(cache_dir)
        target_dir = BRONZE_DIR / f"survey_year={dataset['survey_year']}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / "survey.csv"
        shutil.copy2(source, target)
        written.append(target)
        print(dataset["edition"], "->", target)
    return written


if __name__ == "__main__":
    ingest_bronze()
