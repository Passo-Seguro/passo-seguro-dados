# Databricks notebook source
# MAGIC %md
# MAGIC # Passo Seguro — 05 Export
# MAGIC
# MAGIC **Objetivo.** Gravar a `workspace.gs.vw_prioridades` como **um único arquivo** `painel.json` no Volume `workspace.gs.export`, que é o que `GET /painel` da API serve.
# MAGIC
# MAGIC **Formato.** Array de objetos, UTF-8, ordenado da maior para a menor prioridade (`indice_prioridade` decrescente), como o contrato pede. O arquivo é escrito direto pelo Python no Volume (a view tem 32 linhas), então não existem os arquivos `_SUCCESS` nem `part-*` que o Spark criaria. Cada execução reescreve o mesmo `painel.json`.
# MAGIC
# MAGIC **Contrato.** Hoje o arquivo traz **só as colunas de `PrioridadeSubprefeitura`** do `openapi.yaml` (`GET /painel`): `subprefeitura`, `prioridade`, `relatos_abertos`, `relatos_por_categoria`, `tempo_medio_ate_reparo_dias`, `atualizado_em`. A API responde `503 painel_invalido` se o arquivo sair do schema, por isso o notebook **valida cada objeto contra o contrato antes de gravar** e para sem tocar no arquivo se algo estiver fora.
# MAGIC
# MAGIC **Colunas extras.** `COLUNAS_EXTRAS` lista os campos de análise da view que ainda não estão no contrato (ver `docs/proposta-contrato-painel.md`). Quando o contrato for ampliado, mude **uma linha**: `INCLUIR_EXTRAS = True`.

# COMMAND ----------

import json
import os
import re

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

VIEW = "workspace.gs.vw_prioridades"
DESTINO = "/Volumes/workspace/gs/export"
ARQUIVO = f"{DESTINO}/painel.json"

# Campos de PrioridadeSubprefeitura no contrato atual (openapi.yaml, GET /painel)
COLUNAS_CONTRATO = [
    "subprefeitura",
    "prioridade",
    "relatos_abertos",
    "relatos_por_categoria",
    "tempo_medio_ate_reparo_dias",
    "atualizado_em",
]
# Campos de análise da view que ainda NÃO estão no contrato (proposta em docs/proposta-contrato-painel.md)
COLUNAS_EXTRAS = [
    "reclamacoes_calcada_sp156",
    "reclamacoes_guia_sarjeta_sp156",
    "atropelamentos_pedestres",
    "pedestres_mortos",
    "indice_prioridade",
]
INCLUIR_EXTRAS = False  # <- mude para True quando o contrato for ampliado

PRIORIDADES = {"alta", "media", "baixa"}
CATEGORIAS = ["buraco", "piso_danificado", "obstaculo", "veiculo_na_calcada"]
ISO_COM_FUSO = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})$")

COLUNAS_EXPORTADAS = COLUNAS_CONTRATO + (COLUNAS_EXTRAS if INCLUIR_EXTRAS else [])
print("Colunas exportadas:", COLUNAS_EXPORTADAS)

# COMMAND ----------

# Lê a view, ordena da maior para a menor prioridade e formata atualizado_em como ISO 8601 com fuso (-03:00)
painel = (
    spark.table(VIEW)
    .orderBy(F.col("indice_prioridade").desc(), "subprefeitura")
    .withColumn("atualizado_em", F.date_format("atualizado_em", "yyyy-MM-dd'T'HH:mm:ssXXX"))
    .select(*COLUNAS_EXPORTADAS)
)
registros = [linha.asDict(recursive=True) for linha in painel.collect()]
print(f"{len(registros)} subprefeituras lidas da {VIEW}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validação contra o contrato
# MAGIC Espelha `PrioridadeSubprefeitura`: campos obrigatórios, `prioridade` em `alta`/`media`/`baixa`, inteiros ≥ 0, `relatos_por_categoria` com exatamente as 4 classes, tempo médio número ≥ 0 ou nulo e `atualizado_em` em ISO 8601 com fuso.

# COMMAND ----------

def inteiro_nao_negativo(v):
    return isinstance(v, int) and not isinstance(v, bool) and v >= 0


def numero_nao_negativo(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and v >= 0


def validar(r):
    erros = []
    if set(r) != set(COLUNAS_EXPORTADAS):
        erros.append(f"campos diferentes do esperado: {sorted(set(r) ^ set(COLUNAS_EXPORTADAS))}")
    if not isinstance(r.get("subprefeitura"), str) or not r.get("subprefeitura"):
        erros.append("subprefeitura vazia")
    if r.get("prioridade") not in PRIORIDADES:
        erros.append(f"prioridade inválida: {r.get('prioridade')!r}")
    if not inteiro_nao_negativo(r.get("relatos_abertos")):
        erros.append("relatos_abertos deve ser inteiro >= 0")
    cat = r.get("relatos_por_categoria")
    if not isinstance(cat, dict) or list(cat) != CATEGORIAS or not all(inteiro_nao_negativo(v) for v in cat.values()):
        erros.append(f"relatos_por_categoria deve ter exatamente {CATEGORIAS} com inteiros >= 0")
    tempo = r.get("tempo_medio_ate_reparo_dias")
    if tempo is not None and not numero_nao_negativo(tempo):
        erros.append("tempo_medio_ate_reparo_dias deve ser número >= 0 ou nulo")
    if not isinstance(r.get("atualizado_em"), str) or not ISO_COM_FUSO.match(r["atualizado_em"]):
        erros.append(f"atualizado_em não é ISO 8601 com fuso: {r.get('atualizado_em')!r}")
    if INCLUIR_EXTRAS:
        for campo in ("reclamacoes_calcada_sp156", "reclamacoes_guia_sarjeta_sp156", "atropelamentos_pedestres", "pedestres_mortos"):
            if not inteiro_nao_negativo(r.get(campo)):
                erros.append(f"{campo} deve ser inteiro >= 0")
        indice = r.get("indice_prioridade")
        if not (numero_nao_negativo(indice) and indice <= 1):
            erros.append("indice_prioridade deve ser número entre 0 e 1")
    return erros


problemas = {r.get("subprefeitura"): validar(r) for r in registros if validar(r)}
assert registros, "a view está vazia: rode a 03_ouro antes"
assert not problemas, f"painel fora do contrato, nada foi gravado: {problemas}"
print(f"{len(registros)} objetos válidos contra PrioridadeSubprefeitura")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gravação
# MAGIC Um único `painel.json` (array de objetos, UTF-8). Depois de gravar, o notebook lista o Volume e confere que não há `_SUCCESS` nem `part-*`.

# COMMAND ----------

with open(ARQUIVO, "w", encoding="utf-8") as f:
    json.dump(registros, f, ensure_ascii=False, indent=2)
    f.write("\n")

arquivos = sorted(os.listdir(DESTINO))
print("Conteúdo de", DESTINO, "->", arquivos)
assert arquivos == ["painel.json"], f"esperava só painel.json, achei {arquivos}"
assert not [a for a in arquivos if a.startswith(("_SUCCESS", "part-"))]

# COMMAND ----------

# Conferência: lê de volta o arquivo gravado
with open(ARQUIVO, encoding="utf-8") as f:
    lido = json.load(f)

assert isinstance(lido, list) and len(lido) == len(registros)
assert all(not validar(r) for r in lido)
print(f"painel.json: {os.path.getsize(ARQUIVO):,} bytes, {len(lido)} objetos, UTF-8")
print(json.dumps(lido[:2], ensure_ascii=False, indent=2))
