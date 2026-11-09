"""Open-Meteo: chuva horária em São Paulo (ponto central da cidade) em um dia."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone

import requests

from . import comum

log = logging.getLogger(__name__)

# Praça da Sé. Uma única estação basta para o MVP; a chuva varia pouco dentro do piloto.
LATITUDE, LONGITUDE = -23.5505, -46.6333
FUSO = "America/Sao_Paulo"
# O arquivo histórico (ERA5) tem uns dias de atraso; os dias mais recentes vêm da previsão.
APIS = (
    ("archive", "https://archive-api.open-meteo.com/v1/archive"),
    ("forecast", "https://api.open-meteo.com/v1/forecast"),
)


def _consultar(url: str, dia: date) -> dict:
    resp = requests.get(
        url,
        params={
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "start_date": dia.isoformat(),
            "end_date": dia.isoformat(),
            "hourly": "precipitation",
            "timezone": FUSO,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def coletar(dia: date) -> dict:
    comum.checar_falha_forcada("open_meteo")
    for origem, url in APIS:
        try:
            dados = _consultar(url, dia)
        except requests.HTTPError as exc:
            # 400 = data fora do alcance desta API (ex.: muito antiga para a previsão); tenta a próxima.
            if exc.response is not None and exc.response.status_code == 400 and origem != APIS[-1][0]:
                continue
            if exc.response is not None and exc.response.status_code == 400:
                comum.sem_dados(f"Open-Meteo não tem dados para {dia}.")
            raise
        chuva = dados.get("hourly", {}).get("precipitation", [])
        if any(v is not None for v in chuva):
            break
        log.info("Open-Meteo (%s) sem valores para %s; tentando a próxima API.", origem, dia)
    else:
        comum.sem_dados(f"Open-Meteo sem medições de chuva para {dia}.")

    dados["coleta"] = {
        "fonte": "open-meteo",
        "api": origem,
        "data_referencia": dia.isoformat(),
        "coletado_em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    saida = comum.destino("open_meteo", dia)
    comum.gravar_atomico(saida, lambda p: p.write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8"))
    total = sum(v for v in chuva if v is not None)
    log.info("Open-Meteo %s (%s): %s horas, %.1f mm no dia, gravado em %s.", dia, origem, len(chuva), total, saida)
    return {"arquivo": str(saida), "api": origem, "chuva_total_mm": round(total, 1)}
