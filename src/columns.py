from __future__ import annotations

import re
import unicodedata
from typing import Iterable

# cada ano o kaggle muda o texto do cabeçalho. isso aqui tenta achar a coluna certa.


def normalize(text: object) -> str:
    value = unicodedata.normalize("NFKD", str(text).lower())
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.replace("(", " ").replace(")", " ").replace("'", " ")
    value = re.sub(r"[^a-z0-9.]+", " ", value)
    return " ".join(value.split())


def _has_token(name: str, token: str) -> bool:
    if not token:
        return True
    if " " in token or "." in token:
        return token in name
    return re.search(rf"(?:^| ){re.escape(token)}(?: |$)", name) is not None


def find_column(
    columns: Iterable[str],
    include: Iterable[str],
    exclude: Iterable[str] = (),
    prefer_shortest: bool = True,
) -> str | None:
    include_n = [normalize(item) for item in include]
    exclude_n = [normalize(item) for item in exclude]
    hits: list[str] = []
    for column in columns:
        name = normalize(column)
        if all(_has_token(name, token) for token in include_n) and not any(
            _has_token(name, token) for token in exclude_n
        ):
            hits.append(str(column))
    if not hits:
        return None
    if prefer_shortest:
        hits.sort(key=lambda item: (len(normalize(item)), len(item)))
    return hits[0]


def resolve_schema(columns: Iterable[str]) -> dict[str, str | None]:
    cols = list(columns)
    return {
        "age": find_column(cols, ["idade"], exclude=["faixa"]),
        "age_band": find_column(cols, ["faixa", "idade"]),
        "gender": find_column(cols, ["genero"], exclude=["identidade", "prejudicada"]),
        "race": find_column(cols, ["cor", "raca"]),
        "pcd": find_column(cols, ["pcd"], exclude=["devido", "fato"]),
        "region": find_column(cols, ["regiao", "onde", "mora"]),
        "uf": find_column(cols, ["uf", "onde", "mora"]),
        "education": find_column(cols, ["nivel", "ensino"]),
        "work_status": find_column(cols, ["situacao"], exclude=["entrevista"]),
        "sector": find_column(cols, ["setor"], exclude=["publico", "alimenticio", "imobiliario", "energia", "automotivo"]),
        "job_title": find_column(cols, ["cargo", "atual"]),
        "seniority": find_column(
            cols,
            ["nivel"],
            exclude=["ensino", "cobranca", "vagas"],
        ),
        "salary_band": find_column(cols, ["faixa", "salarial"]),
        "experience": find_column(cols, ["experiencia"], exclude=["ti", "prejudicada", "processos", "profissional"]),
        "work_model": _work_model(cols),
        "uses_python": find_column(cols, ["python"], exclude=["pipeline", "programacao", "scripts", "processo"]),
        "uses_sql": _sql_flag(cols),
        "uses_aws": find_column(cols, ["amazon", "web", "services"]),
        "uses_gcp": find_column(cols, ["google", "cloud"]),
        "uses_azure": find_column(cols, ["azure", "microsoft"], exclude=["machine", "learning"]),
        "uses_powerbi": _powerbi(cols),
        "is_manager": find_column(cols, ["p2 d", "gestor"])
        or find_column(cols, ["2.d", "gestor"])
        or find_column(cols, ["atua", "como", "gestor"]),
        "uses_tableau": find_column(cols, ["tableau"], exclude=["dashboards", "ferramentas", "powerbi"]),
        "uses_databricks": find_column(cols, ["databricks"], exclude=["feature", "store"]),
        "uses_snowflake": find_column(cols, ["snowflake"]),
        "job_function": find_column(cols, ["funcao"], exclude=["atuacao em dados"]),
        "genai_not_used": find_column(cols, ["nao uso", "ai generativa"])
        or find_column(cols, ["nao utilizo", "ia generativa"]),
        "genai_free": find_column(cols, ["solucoes gratuitas", "ai generativa"]),
        "genai_paid_self": find_column(cols, ["pago pelas solucoes", "ai generativa"])
        or find_column(cols, ["pago do meu"]),
        "genai_paid_company": find_column(cols, ["empresa", "paga", "ai generativa"]),
        "genai_copilot": find_column(cols, ["copilot"], exclude=["desenvolvedores", "equipes"]),
        "genai_use_text": find_column(cols, ["chatgpt"], exclude=["gratuitas", "pagas"]),
        "company_genai_priority": _company_priority(cols),
    }


def _work_model(columns: list[str]) -> str | None:
    return (
        find_column(columns, ["modelo", "trabalho", "atual"])
        or find_column(columns, ["atualmente", "forma", "trabalho"], exclude=["ideal"])
        or find_column(columns, ["forma", "trabalho"], exclude=["ideal"])
    )


def _sql_flag(columns: list[str]) -> str | None:
    return (
        find_column(columns, ["4.c.1 sql"])
        or find_column(columns, ["4.d.1 sql"])
        or find_column(columns, ["p4 d 1", "sql"])
        or find_column(
            columns,
            ["sql"],
            exclude=["server", "sqlite", "nosql", "mysql", "banco", "relacional", "procedures", "consulta"],
        )
    )


def _company_priority(columns: list[str]) -> str | None:
    return (
        find_column(columns, ["e uma prioridade"])
        or find_column(columns, ["ai generativa e llm e uma prioridade"])
        or find_column(columns, ["p3 e"])
        or find_column(columns, ["3.e ai"])
    )


def _powerbi(columns: list[str]) -> str | None:
    ranked: list[str] = []
    for column in columns:
        name = normalize(column)
        compact = name.replace(" ", "")
        if "powerbi" not in compact and "power bi" not in name:
            continue
        if "dashboard" in name or "ferramenta" in name:
            continue
        ranked.append(column)
    for column in ranked:
        name = normalize(column)
        if (
            name.startswith("4.j.1 ")
            or name.startswith("4.g.1 ")
            or "p4 j 1 " in name
            or "p4 i 1 " in name
            or "p4 g 1 " in name
        ):
            return column
    return ranked[0] if ranked else None
