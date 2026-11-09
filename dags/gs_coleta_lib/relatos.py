"""Relatos do Passo Seguro (app e robô) criados em um dia.

Com a Variable PASSO_SEGURO_API_URL preenchida, lê `GET /relatos` da API (paginado, com o
token da Variable PASSO_SEGURO_API_TOKEN). Com ela vazia, usa um MOCK determinístico:
relatos fictícios, todos marcados com `"simulado": true`, no formato do contrato (openapi.yaml).
"""

from __future__ import annotations

import json
import logging
import random
import uuid
from datetime import date, datetime, timedelta, timezone

import requests
from airflow.models import Variable

from . import comum

log = logging.getLogger(__name__)

FUSO_SP = timezone(timedelta(hours=-3))  # o Brasil não tem horário de verão desde 2019
TAMANHO_PAGINA = 100
CATEGORIAS = ("buraco", "piso_danificado", "obstaculo", "veiculo_na_calcada")
DESCRICOES = {
    "buraco": "Buraco na calçada, difícil passar com carrinho.",
    "piso_danificado": "Lajes soltas, quase caí.",
    "obstaculo": "Entulho ocupando toda a calçada.",
    "veiculo_na_calcada": "Carro estacionado em cima da calçada.",
}
STATUS = (("novo", 40), ("revisao_humana", 8), ("validado", 10), ("encaminhado", 25), ("resolvido", 12), ("rejeitado", 5))
# Caixas (lat_min, lat_max, lon_min, lon_max) das áreas-piloto: Sé e Pinheiros.
AREAS = ((-23.560, -23.540, -46.650, -46.620), (-23.575, -23.555, -46.710, -46.680))
SUBPREFEITURAS = {0: "SE", 1: "PINHEIROS"}


def _mock(dia: date) -> list[dict]:
    rng = random.Random(f"relatos-mock-{dia.isoformat()}")  # mesmo dia, mesmo resultado
    inicio = datetime(dia.year, dia.month, dia.day, tzinfo=FUSO_SP)
    itens = []
    for _ in range(rng.randint(8, 30)):
        area = rng.randrange(len(AREAS))
        lat_min, lat_max, lon_min, lon_max = AREAS[area]
        categoria = None if rng.random() < 0.05 else rng.choice(CATEGORIAS)
        status = rng.choices([s for s, _ in STATUS], [p for _, p in STATUS])[0]
        tratado = status in ("encaminhado", "resolvido")
        id_relato = uuid.UUID(int=rng.getrandbits(128), version=4)
        itens.append(
            {
                "id": str(id_relato),
                "criado_em": (inicio + timedelta(seconds=rng.randrange(86400))).isoformat(),
                "lat": round(rng.uniform(lat_min, lat_max), 6),
                "lon": round(rng.uniform(lon_min, lon_max), 6),
                "foto_url": f"https://fotos.exemplo.invalid/relatos/{str(id_relato)[:8]}.jpg",
                "descricao": None if rng.random() < 0.3 or categoria is None else DESCRICOES[categoria],
                "categoria": categoria,
                "confianca": None if categoria is None else round(rng.uniform(0.35, 0.99), 2),
                "fonte": "app" if rng.random() < 0.85 else "robo",
                "status": status,
                "confirmacoes": rng.randint(1, 12) if tratado else rng.randint(0, 3),
                "subprefeitura": SUBPREFEITURAS[area] if tratado else None,
                "prioridade": rng.choice(("alta", "media", "baixa")) if tratado else None,
                "simulado": True,
            }
        )
    return sorted(itens, key=lambda r: r["criado_em"])


def _da_api(url_base: str, dia: date) -> list[dict]:
    """Todas as páginas de GET /relatos criados no dia (a API ordena do mais antigo ao mais recente)."""
    inicio = datetime(dia.year, dia.month, dia.day, tzinfo=FUSO_SP)
    fim = inicio + timedelta(days=1)
    token = Variable.get("PASSO_SEGURO_API_TOKEN", default_var="")
    cabecalhos = {"Authorization": f"Bearer {token}"} if token else {}
    itens, pagina = [], 1
    while True:
        resp = requests.get(
            f"{url_base.rstrip('/')}/relatos",
            params={"criado_desde": inicio.isoformat(), "pagina": pagina, "tamanho": TAMANHO_PAGINA},
            headers=cabecalhos,
            timeout=30,
        )
        resp.raise_for_status()
        corpo = resp.json()
        itens += corpo["itens"]
        if not corpo["paginacao"]["tem_proxima"]:
            break
        pagina += 1
    return [r for r in itens if inicio <= datetime.fromisoformat(r["criado_em"]) < fim]


def coletar(dia: date) -> dict:
    comum.checar_falha_forcada("relatos")
    url_api = Variable.get("PASSO_SEGURO_API_URL", default_var="").strip()
    origem = "api" if url_api else "mock"
    itens = _da_api(url_api, dia) if url_api else _mock(dia)

    corpo = {"data_referencia": dia.isoformat(), "origem": origem, "total": len(itens), "itens": itens}
    saida = comum.destino("relatos", dia)
    comum.gravar_atomico(saida, lambda p: p.write_text(json.dumps(corpo, ensure_ascii=False, indent=2), encoding="utf-8"))
    log.info("Relatos %s (%s): %s relatos gravados em %s.", dia, origem, len(itens), saida)
    return {"arquivo": str(saida), "origem": origem, "total": len(itens)}
