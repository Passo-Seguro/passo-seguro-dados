# Databricks notebook source
# MAGIC %md
# MAGIC # Passo Seguro — 01 Bronze
# MAGIC
# MAGIC **Objetivo.** Copiar os arquivos que o Airflow deixou em `chegada/<fonte>/` para tabelas Delta **sem transformar nada**: o conteúdo continua como veio, em texto. Cada linha ganha só dois campos de rastreio: `arquivo_origem` (de qual arquivo veio) e `carregado_em` (quando entrou na Bronze). Tipos, datas, coordenadas e o `explode` dos JSONs ficam para a Prata.
# MAGIC
# MAGIC | Fonte | Pasta em `chegada/` | Formato | Tabela |
# MAGIC |---|---|---|---|
# MAGIC | SP156 | `sp156/` | CSV (`;`, UTF-8) | `workspace.gs.bronze_sp156` |
# MAGIC | Infosiga | `infosiga/` | CSV (`;`, UTF-8) | `workspace.gs.bronze_infosiga` |
# MAGIC | Open-Meteo | `open_meteo/` | JSON (um objeto por arquivo, indentado) | `workspace.gs.bronze_open_meteo` |
# MAGIC | Relatos | `relatos/` | JSON (um objeto por arquivo, indentado; simulados) | `workspace.gs.bronze_relatos` |
# MAGIC
# MAGIC **Por que reexecutar não duplica.** A leitura usa o Auto Loader (`cloudFiles`). Cada fonte tem um *checkpoint* em `chegada/_controle/checkpoint_<fonte>` que guarda a lista dos arquivos já lidos. Numa nova execução o Auto Loader lê **só os arquivos que ainda não estão no checkpoint** (os do próximo lote) e acrescenta as linhas novas; reexecutar sem arquivo novo não lê nada. Para reprocessar uma fonte do zero, apague a tabela e as pastas `schema_<fonte>` e `checkpoint_<fonte>` em `_controle/`.
# MAGIC
# MAGIC **Pontos de atenção**
# MAGIC - **Nomes de colunas dos CSVs:** o SP156 tem colunas como `Data de Finalização` e `Serviço`, que o Delta não aceita sem *column mapping*. Os nomes são normalizados (minúsculas, sem acento, `_` no lugar de espaço): `Data de Finalização` → `data_de_finalizacao`. Os valores não são tocados.
# MAGIC - **Arquivo só com cabeçalho** (ex.: um dia do Infosiga sem sinistro com pedestre) é lido sem erro, mas **não gera linha**; por isso não aparece em `arquivos distintos` na contagem final.
# MAGIC - **JSONs:** cada arquivo vira **uma linha** (`linhas` = nº de arquivos); `hourly` (Open-Meteo) e `itens` (Relatos) continuam como arrays aninhados.
# MAGIC - `_rescued_data` recebe o que não coube no schema (deve ficar vazio).

# COMMAND ----------

import re
import unicodedata

from pyspark.sql import functions as F

CATALOGO = "workspace"
SCHEMA = "gs"
base = "/Volumes/workspace/gs/chegada"

FONTES = ["sp156", "infosiga", "open_meteo", "relatos"]


def tabela_bronze(fonte):
    return f"{CATALOGO}.{SCHEMA}.bronze_{fonte}"


def nome_coluna(nome):
    """'Data de Finalização' -> 'data_de_finalizacao'. Colunas técnicas (_rescued_data) ficam como estão."""
    if nome.startswith("_"):
        return nome
    sem_acento = unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "_", sem_acento.lower()).strip("_")


def pasta_tem_arquivos(pasta):
    return any(not f.isDir() for f in dbutils.fs.ls(pasta))

# COMMAND ----------

def _ingerir(fonte, opcoes_leitura, normalizar_colunas=False):
    """Padrão único da Bronze: Auto Loader -> Delta, processa o que chegou e termina."""
    pasta = f"{base}/{fonte}"
    if not pasta_tem_arquivos(pasta):
        print(f"[{fonte}] nenhum arquivo em {pasta}; nada a fazer.")
        return

    leitor = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.schemaLocation", f"{base}/_controle/schema_{fonte}")
        .option("cloudFiles.inferColumnTypes", "false")  # tudo string; tipos ficam para a Prata
    )
    for chave, valor in opcoes_leitura.items():
        leitor = leitor.option(chave, valor)
    df = leitor.load(pasta)

    if normalizar_colunas:
        df = df.toDF(*[nome_coluna(c) for c in df.columns])

    df = df.select(
        "*",
        F.col("_metadata.file_path").alias("arquivo_origem"),
        F.current_timestamp().alias("carregado_em"),
    )

    consulta = (
        df.writeStream
        .option("checkpointLocation", f"{base}/_controle/checkpoint_{fonte}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(tabela_bronze(fonte))
    )
    consulta.awaitTermination()  # só segue para a próxima fonte quando esta terminar
    print(f"[{fonte}] ok -> {tabela_bronze(fonte)}")


def ingerir_sp156():
    # CSV ';' em UTF-8, com cabeçalho, sem aspas nem quebra de linha dentro de campo (conferido nos arquivos reais)
    _ingerir(
        "sp156",
        {"cloudFiles.format": "csv", "header": "true", "sep": ";", "encoding": "UTF-8"},
        normalizar_colunas=True,
    )


def ingerir_infosiga():
    # Mesmo formato do SP156. latitude/longitude seguem com vírgula decimal, como vêm
    _ingerir(
        "infosiga",
        {"cloudFiles.format": "csv", "header": "true", "sep": ";", "encoding": "UTF-8"},
        normalizar_colunas=True,
    )


def ingerir_open_meteo():
    # JSON indentado (objeto em várias linhas) -> multiLine. Estrutura aninhada preservada
    _ingerir("open_meteo", {"cloudFiles.format": "json", "multiLine": "true"})


def ingerir_relatos():
    # JSON indentado (objeto em várias linhas) -> multiLine. 'itens' continua array; o explode é na Prata
    _ingerir("relatos", {"cloudFiles.format": "json", "multiLine": "true"})

# COMMAND ----------

ingerir_sp156()

# COMMAND ----------

ingerir_infosiga()

# COMMAND ----------

ingerir_open_meteo()

# COMMAND ----------

ingerir_relatos()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contagem por fonte
# MAGIC Tire o print desta tabela **antes do lote 2**, **depois dele** e **depois de reexecutar** (nesse último caso os números não mudam, só `maior carregado_em` deve continuar igual).
# MAGIC `arquivo mais recente` é o maior nome de arquivo (a data está no nome, em formato AAAA-MM-DD).

# COMMAND ----------

nome_arquivo = F.regexp_extract("arquivo_origem", r"([^/]+)$", 1)

resumos = []
for fonte in FONTES:
    tabela = tabela_bronze(fonte)
    if not spark.catalog.tableExists(tabela):
        print(f"[{fonte}] tabela {tabela} ainda não existe.")
        continue
    resumos.append(
        spark.table(tabela).agg(
            F.lit(fonte).alias("fonte"),
            F.count(F.lit(1)).alias("linhas"),
            F.countDistinct("arquivo_origem").alias("arquivos distintos"),
            F.max(nome_arquivo).alias("arquivo mais recente"),
            F.max("carregado_em").alias("maior carregado_em"),
        )
    )

if resumos:
    contagem = resumos[0]
    for r in resumos[1:]:
        contagem = contagem.unionByName(r)
    display(contagem)
