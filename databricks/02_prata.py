# Databricks notebook source
# MAGIC %md
# MAGIC # Passo Seguro — 02 Prata
# MAGIC
# MAGIC **Objetivo.** Ler a Bronze (texto, como veio) e entregar tabelas **tipadas, limpas e sem duplicata**, prontas para a Ouro. Cada fonte vira uma tabela `workspace.gs.prata_<fonte>`; o que é barrado por uma regra de qualidade vai para `workspace.gs.prata_rejeitados`, com o motivo.
# MAGIC
# MAGIC | Fonte | Bronze | Prata | Chave | Uma linha por |
# MAGIC |---|---|---|---|---|
# MAGIC | SP156 | `bronze_sp156` | `prata_sp156` | `id_solicitacao` (derivada, ver abaixo) | solicitação |
# MAGIC | Infosiga | `bronze_infosiga` | `prata_infosiga` | `id_sinistro` | sinistro com pedestre |
# MAGIC | Open-Meteo | `bronze_open_meteo` | `prata_open_meteo` | `data_hora` | hora (`hourly` explodido) |
# MAGIC | Relatos | `bronze_relatos` | `prata_relatos` | `id` | relato (`itens` explodido) |
# MAGIC
# MAGIC **Fluxo de cada fonte:** ler a Bronze **inteira** → tipar e limpar → deduplicar pela chave (fica o registro de `carregado_em` mais recente) → aplicar as regras de qualidade → `MERGE` dos aceitos em `prata_<fonte>` e dos barrados em `prata_rejeitados`.
# MAGIC
# MAGIC **Por que reexecutar não duplica.** A Prata sempre relê toda a Bronze e grava por `MERGE` na chave (`WHEN MATCHED UPDATE SET *`, `WHEN NOT MATCHED INSERT *`): linha que já existe é atualizada com o mesmo valor, linha nova é inserida. Os barrados também entram por `MERGE`, na chave `(fonte, chave, regra)`, então a contagem de rejeitados não cresce a cada execução. Depois do lote 2 (01/09 em diante) basta rodar de novo: só as linhas novas entram.
# MAGIC
# MAGIC **Regras de qualidade** (o registro barrado **não** entra em `prata_<fonte>`):
# MAGIC
# MAGIC | Regra | Quando barra | Fontes |
# MAGIC |---|---|---|
# MAGIC | `coordenada_fora_do_municipio` | há coordenada **e** ela cai fora de lat -24,01 a -23,35 / lon -46,83 a -46,36 | sp156, infosiga, relatos |
# MAGIC | `data_no_futuro` | a data do registro é posterior ao momento da execução | todas |
# MAGIC | `categoria_ausente_ou_fora_da_lista` | relato com `categoria` nula ou fora de buraco / piso_danificado / obstaculo / veiculo_na_calcada; SP156 com serviço nulo ou fora de `SERVICOS_CALCADA` | relatos, sp156 |
# MAGIC
# MAGIC Sem coordenada **não** é motivo de rejeição: o sinistro do Infosiga sem lat/lon fica na Prata com `tem_coordenada = false`. Já a categoria **nula** é motivo de rejeição (relato sem classe não serve para a Ouro).
# MAGIC
# MAGIC **Pontos de atenção**
# MAGIC - **O SP156 não tem número de protocolo.** A coluna `numero` é o número do endereço. Por isso `id_solicitacao` é derivado: hash SHA-256 dos campos que não mudam com o tempo (abertura, canal, tema, assunto, serviço, órgão, endereço, CEP, coordenadas, prefeitura, distrito) mais um contador para linhas que repetem todos esses campos (no lote 1 eram 455 linhas; são mantidas, não colapsadas). `status` e `data_de_finalizacao` ficam **fora** da chave de propósito, para a solicitação ser atualizada, e não duplicada, quando for finalizada.
# MAGIC - **Open-Meteo:** a coleta pede só `precipitation`, então a tabela tem só `precipitacao_mm` (sem temperatura).
# MAGIC - **Fuso:** `spark.sql.session.timeZone = America/Sao_Paulo`; horas do Open-Meteo e `criado_em` sem fuso são lidos nele.

# COMMAND ----------

from datetime import datetime
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

CATALOGO = "workspace"
SCHEMA = "gs"
FONTES = ["sp156", "infosiga", "open_meteo", "relatos"]
TABELA_REJEITADOS = f"{CATALOGO}.{SCHEMA}.prata_rejeitados"

# Momento da execução: lido uma vez, para a regra data_no_futuro valer igual em todas as fontes
AGORA = spark.sql("SELECT current_timestamp() AS agora").first()["agora"]
print("Execução:", AGORA)

# Caixa do município de São Paulo (as mesmas usadas na validação dos relatos)
LAT_MIN, LAT_MAX = -24.01, -23.35
LON_MIN, LON_MAX = -46.83, -46.36

# Cópia de dags/gs_coleta_lib/sp156.py (o notebook não enxerga o repositório): manter as duas iguais
SERVICOS_CALCADA = (
    "Calçada pública - solicitar manutenção",
    "Calçada particular - denunciar calçada danificada ou inexistente",
    "Acessibilidade - solicitar avaliação de obstáculo na calçada",
    "Guias, sarjetas e sarjetões - solicitar manutenção",
)
SERVICO_GUIA_SARJETA = "Guias, sarjetas e sarjetões - solicitar manutenção"
CATEGORIAS_RELATO = ("buraco", "piso_danificado", "obstaculo", "veiculo_na_calcada")

# Status do SP156 (valor em minúsculas) -> status padronizado. Valor fora daqui fica NULL e é avisado.
MAPA_STATUS = {
    "finalizada": "finalizada",
    "cancelada": "cancelada",  # status próprio: a fonte não tem "Indeferida"
    "aberto": "em_aberto",
    "em andamento": "em_aberto",
}

# Regras que se aplicam a cada fonte (também monta a tabela de barrados, com os zeros)
REGRAS = {
    "sp156": ["coordenada_fora_do_municipio", "data_no_futuro", "categoria_ausente_ou_fora_da_lista"],
    "infosiga": ["coordenada_fora_do_municipio", "data_no_futuro"],
    "open_meteo": ["data_no_futuro"],
    "relatos": ["coordenada_fora_do_municipio", "data_no_futuro", "categoria_ausente_ou_fora_da_lista"],
}


def bronze(fonte):
    return f"{CATALOGO}.{SCHEMA}.bronze_{fonte}"


def prata(fonte):
    return f"{CATALOGO}.{SCHEMA}.prata_{fonte}"

# COMMAND ----------

# Funções de tratamento. Todas usam try_*: valor que não converte vira NULL em vez de derrubar o notebook (ANSI)

def txt(coluna):
    """Texto com trim; vazio vira NULL."""
    aparado = F.trim(F.col(coluna))
    return F.when(aparado == "", None).otherwise(aparado)


def para_double(coluna):
    """'-23,5019' ou '-23.5019' -> double; vazio ou inválido -> NULL."""
    return F.expr(f"try_cast(replace(trim({coluna}), ',', '.') as double)")


def para_int(coluna):
    return F.expr(f"try_cast(trim({coluna}) as int)")


def para_data(coluna, formato="dd/MM/yyyy"):
    return F.expr(f"try_to_date(trim({coluna}), '{formato}')")


def mapear(coluna, mapa):
    """Mapa de valores (comparando em minúsculas); o que não está no mapa vira NULL."""
    expressao = None
    for origem, destino in mapa.items():
        condicao = F.lower(coluna) == origem
        expressao = F.when(condicao, destino) if expressao is None else expressao.when(condicao, destino)
    return expressao


def deduplicar(df, chave):
    """Uma linha por chave: a de carregado_em mais recente (arquivo mais recente desempata)."""
    janela = Window.partitionBy(chave).orderBy(F.col("carregado_em").desc(), F.col("arquivo_origem").desc())
    return df.withColumn("_rn", F.row_number().over(janela)).filter("_rn = 1").drop("_rn")


def fora_do_municipio(lat, lon):
    """Só barra quando HÁ coordenada e ela cai fora da caixa do município."""
    dentro = lat.between(LAT_MIN, LAT_MAX) & lon.between(LON_MIN, LON_MAX)
    return lat.isNotNull() & lon.isNotNull() & ~dentro


DETALHE_COORDENADA = F.lit(
    f"lat/lon fora de lat {LAT_MIN} a {LAT_MAX} e lon {LON_MIN} a {LON_MAX}"
)
DETALHE_FUTURO = F.lit("data do registro posterior ao momento da execução")

# COMMAND ----------

# Gravação: MERGE idempotente nas tabelas da Prata

def criar_se_nao_existe(tabela, df):
    if not spark.catalog.tableExists(tabela):
        df.limit(0).write.format("delta").saveAsTable(tabela)


def merge_prata(fonte, df, chave):
    tabela = prata(fonte)
    criar_se_nao_existe(tabela, df)
    df.createOrReplaceTempView(f"_origem_{fonte}")
    spark.sql(f"""
        MERGE INTO {tabela} AS t
        USING _origem_{fonte} AS s
        ON t.{chave} = s.{chave}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABELA_REJEITADOS} (
        fonte STRING, chave STRING, regra STRING, valor STRING, detalhe STRING, carregado_em TIMESTAMP
    )
""")


def merge_rejeitados(fonte, rejeitados):
    """MERGE por (fonte, chave, regra). O que deixou de ser barrado (ex.: data que deixou de ser futura)
    sai da tabela (NOT MATCHED BY SOURCE), restrito à fonte; assim um registro nunca fica aceito e rejeitado."""
    rejeitados.dropDuplicates(["fonte", "chave", "regra"]).createOrReplaceTempView(f"_rejeitados_{fonte}")
    spark.sql(f"""
        MERGE INTO {TABELA_REJEITADOS} AS t
        USING _rejeitados_{fonte} AS s
        ON t.fonte = s.fonte AND t.chave = s.chave AND t.regra = s.regra
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE AND t.fonte = '{fonte}' THEN DELETE
    """)


def aplicar_regras(fonte, df, chave, regras):
    """regras: lista de (nome, condição de barrar, valor, detalhe). Devolve (aceitos, rejeitados)."""
    reprovou = None
    barrados = []
    for nome, condicao, valor, detalhe in regras:
        condicao = F.coalesce(condicao, F.lit(False))
        reprovou = condicao if reprovou is None else (reprovou | condicao)
        barrados.append(
            df.filter(condicao).select(
                F.lit(fonte).alias("fonte"),
                F.col(chave).cast("string").alias("chave"),
                F.lit(nome).alias("regra"),
                valor.cast("string").alias("valor"),
                detalhe.alias("detalhe"),
                F.col("carregado_em"),
            )
        )
    assert [r[0] for r in regras] == REGRAS[fonte], f"[{fonte}] regras diferentes de REGRAS"
    return df.filter(~reprovou), reduce(DataFrame.unionByName, barrados)


def gravar(fonte, df, chave, regras):
    aceitos, rejeitados = aplicar_regras(fonte, df, chave, regras)
    merge_prata(fonte, aceitos, chave)
    merge_rejeitados(fonte, rejeitados)
    print(f"[{fonte}] ok -> {prata(fonte)} + {TABELA_REJEITADOS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SP156
# MAGIC Colunas da Bronze (já normalizadas): `data_de_abertura`, `data_de_finalizacao` (`DD/MM/AAAA`), `meio_de_comunicacao`, `tema`, `assunto`, `servico`, `status`, `orgao`, `logradouro`, `numero`, `cep`, `latitude`, `longitude` (ponto decimal), `prefeitura_operacional`, `distrito`. **Não há coluna de protocolo** (ver nota no topo).
# MAGIC
# MAGIC Status distintos na Bronze e como cada um é padronizado (confira antes de seguir):

# COMMAND ----------

status_distintos = (
    spark.table(bronze("sp156"))
    .select(txt("status").alias("status_original"))
    .groupBy("status_original").count()
    .withColumn("status_padronizado", mapear(F.col("status_original"), MAPA_STATUS))
    .orderBy(F.col("count").desc())
)
display(status_distintos)

sem_mapa = status_distintos.filter("status_original IS NOT NULL AND status_padronizado IS NULL").collect()
if sem_mapa:
    print("ATENÇÃO: status sem mapeamento (ficam NULL na Prata):", [r["status_original"] for r in sem_mapa])

# COMMAND ----------

# Campos que não mudam com o tempo: formam a chave derivada (status e finalização ficam de fora)
CAMPOS_FIXOS_SP156 = [
    "data_de_abertura", "meio_de_comunicacao", "tema", "assunto", "servico", "orgao", "logradouro",
    "numero", "cep", "latitude", "longitude", "prefeitura_operacional", "distrito",
]

b = spark.table(bronze("sp156")).select(
    *[txt(c).alias(c) for c in CAMPOS_FIXOS_SP156 + ["status", "data_de_finalizacao"]],
    "arquivo_origem",
    "carregado_em",
)

conteudo = F.sha2(F.concat_ws("\u001f", *[F.coalesce(F.col(c), F.lit("")) for c in CAMPOS_FIXOS_SP156]), 256)
# Linhas idênticas em todos os campos fixos ganham contador 1, 2, 3... (ordem estável) e continuam sendo linhas distintas
janela_repetidas = Window.partitionBy("_conteudo").orderBy("data_de_finalizacao", "status", "arquivo_origem")

sp156 = (
    b.withColumn("_conteudo", conteudo)
    .withColumn("_n", F.row_number().over(janela_repetidas))
    .select(
        F.sha2(F.concat_ws("#", F.col("_conteudo"), F.col("_n")), 256).alias("id_solicitacao"),
        para_data("data_de_abertura").alias("data_abertura"),
        para_data("data_de_finalizacao").alias("data_finalizacao"),
        F.col("meio_de_comunicacao"),
        F.col("tema"),
        F.col("assunto"),
        F.col("servico"),
        F.when(F.col("servico") == SERVICO_GUIA_SARJETA, "guia_sarjeta")
        .when(F.col("servico").isin(*SERVICOS_CALCADA), "calcada")
        .alias("categoria"),
        F.col("status").alias("status_original"),
        mapear(F.col("status"), MAPA_STATUS).alias("status"),
        F.col("orgao"),
        F.col("logradouro"),
        F.col("numero"),
        F.when(F.length(F.regexp_replace("cep", r"\D", "")) == 8, F.regexp_replace("cep", r"\D", "")).alias("cep"),
        para_double("latitude").alias("latitude"),
        para_double("longitude").alias("longitude"),
        F.col("prefeitura_operacional"),
        F.col("distrito"),
        F.col("arquivo_origem"),
        F.col("carregado_em"),
    )
)
sp156 = deduplicar(sp156, "id_solicitacao")

gravar("sp156", sp156, "id_solicitacao", [
    ("coordenada_fora_do_municipio",
     fora_do_municipio(F.col("latitude"), F.col("longitude")),
     F.concat_ws(",", F.col("latitude"), F.col("longitude")), DETALHE_COORDENADA),
    ("data_no_futuro",
     F.col("data_abertura").cast("timestamp") > F.lit(AGORA),
     F.col("data_abertura"), DETALHE_FUTURO),
    ("categoria_ausente_ou_fora_da_lista",
     F.col("servico").isNull() | ~F.col("servico").isin(*SERVICOS_CALCADA),
     F.col("servico"), F.lit("serviço nulo ou fora de SERVICOS_CALCADA")),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Infosiga
# MAGIC Colunas da Bronze: `id_sinistro`, `data_sinistro` (`DD/MM/AAAA`), `hora` (`HH:mm`), `municipio`, `logradouro`, `latitude`/`longitude` (**vírgula decimal**, vazias em ~8%), `qtd_pedestres`, `qtd_pedestres_feridos`, `qtd_pedestres_mortos`.
# MAGIC
# MAGIC `hora` fica como texto `HH:mm` validado (o Spark não tem tipo só-hora) e `data_hora_sinistro` junta data e hora em `timestamp`. Sem coordenada o sinistro **fica**, com `tem_coordenada = false`.

# COMMAND ----------

infosiga = (
    spark.table(bronze("infosiga"))
    .select(
        txt("id_sinistro").alias("id_sinistro"),
        para_data("data_sinistro").alias("data_sinistro"),
        F.when(txt("hora").rlike(r"^\d{2}:\d{2}$"), txt("hora")).alias("hora"),
        txt("municipio").alias("municipio"),
        txt("logradouro").alias("logradouro"),
        para_double("latitude").alias("latitude"),
        para_double("longitude").alias("longitude"),
        para_int("qtd_pedestres").alias("qtd_pedestres"),
        para_int("qtd_pedestres_feridos").alias("qtd_pedestres_feridos"),
        para_int("qtd_pedestres_mortos").alias("qtd_pedestres_mortos"),
        "arquivo_origem",
        "carregado_em",
    )
    .withColumn(
        "data_hora_sinistro",
        F.expr("try_to_timestamp(concat(date_format(data_sinistro, 'yyyy-MM-dd'), ' ', hora), 'yyyy-MM-dd HH:mm')"),
    )
    .withColumn("tem_coordenada", F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
    .select(
        "id_sinistro", "data_sinistro", "hora", "data_hora_sinistro", "municipio", "logradouro",
        "latitude", "longitude", "tem_coordenada",
        "qtd_pedestres", "qtd_pedestres_feridos", "qtd_pedestres_mortos",
        "arquivo_origem", "carregado_em",
    )
)
infosiga = deduplicar(infosiga, "id_sinistro")

gravar("infosiga", infosiga, "id_sinistro", [
    ("coordenada_fora_do_municipio",
     fora_do_municipio(F.col("latitude"), F.col("longitude")),
     F.concat_ws(",", F.col("latitude"), F.col("longitude")), DETALHE_COORDENADA),
    ("data_no_futuro",
     F.coalesce(F.col("data_hora_sinistro"), F.col("data_sinistro").cast("timestamp")) > F.lit(AGORA),
     F.coalesce(F.col("data_hora_sinistro"), F.col("data_sinistro")), DETALHE_FUTURO),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Open-Meteo
# MAGIC Na Bronze, `hourly` é uma **string JSON** com os arrays paralelos `time` e `precipitation` (24 posições por arquivo). A Prata faz `from_json`, junta os arrays com `arrays_zip` e usa `explode`: uma linha por hora.

# COMMAND ----------

SCHEMA_HOURLY = "struct<time:array<string>,precipitation:array<double>>"

open_meteo = (
    spark.table(bronze("open_meteo"))
    .select(F.from_json("hourly", SCHEMA_HOURLY).alias("h"), "arquivo_origem", "carregado_em")
    .select(
        F.col("h.time").alias("time"),
        F.col("h.precipitation").alias("precipitation"),
        "arquivo_origem",
        "carregado_em",
    )
    .select(
        F.explode(F.arrays_zip("time", "precipitation")).alias("z"),
        "arquivo_origem",
        "carregado_em",
    )
    .select(
        F.expr("try_to_timestamp(replace(z.time, 'T', ' '), 'yyyy-MM-dd HH:mm')").alias("data_hora"),
        F.lit("Sao Paulo").alias("cidade"),
        F.col("z.precipitation").alias("precipitacao_mm"),
        "arquivo_origem",
        "carregado_em",
    )
)
open_meteo = deduplicar(open_meteo, "data_hora")

gravar("open_meteo", open_meteo, "data_hora", [
    ("data_no_futuro", F.col("data_hora") > F.lit(AGORA), F.col("data_hora"), DETALHE_FUTURO),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relatos
# MAGIC Na Bronze, `itens` é uma **string JSON** com o array de relatos do dia (formato `Relato` do contrato da API). Cada item tem `id`, `criado_em` (ISO 8601 com fuso), `lat`, `lon`, `foto_url`, `descricao`, `categoria`, `confianca`, `fonte`, `status`, `confirmacoes`, `subprefeitura`, `prioridade`, `simulado`. O campo da classe do relato se chama **`categoria`**. `simulado` e `origem` (`mock`/`api`) são preservados.
# MAGIC
# MAGIC Os itens são lidos como texto e convertidos com `try_cast`, para um valor ruim virar `NULL` sem derrubar o dia inteiro.

# COMMAND ----------

SCHEMA_ITENS = (
    "array<struct<id:string,criado_em:string,lat:string,lon:string,foto_url:string,descricao:string,"
    "categoria:string,confianca:string,fonte:string,status:string,confirmacoes:string,"
    "subprefeitura:string,prioridade:string,simulado:string>>"
)

relatos = (
    spark.table(bronze("relatos"))
    .select(F.explode(F.from_json("itens", SCHEMA_ITENS)).alias("r"), "origem", "arquivo_origem", "carregado_em")
    .select("r.*", "origem", "arquivo_origem", "carregado_em")
    .select(
        txt("id").alias("id"),
        F.expr("try_cast(criado_em as timestamp)").alias("criado_em"),
        para_double("lat").alias("lat"),
        para_double("lon").alias("lon"),
        txt("foto_url").alias("foto_url"),
        txt("descricao").alias("descricao"),
        txt("categoria").alias("categoria"),
        para_double("confianca").alias("confianca"),
        txt("fonte").alias("fonte"),
        txt("status").alias("status"),
        para_int("confirmacoes").alias("confirmacoes"),
        txt("subprefeitura").alias("subprefeitura"),
        txt("prioridade").alias("prioridade"),
        F.expr("try_cast(trim(simulado) as boolean)").alias("simulado"),
        txt("origem").alias("origem"),
        "arquivo_origem",
        "carregado_em",
    )
)
relatos = deduplicar(relatos, "id")

gravar("relatos", relatos, "id", [
    ("coordenada_fora_do_municipio",
     fora_do_municipio(F.col("lat"), F.col("lon")),
     F.concat_ws(",", F.col("lat"), F.col("lon")), DETALHE_COORDENADA),
    ("data_no_futuro", F.col("criado_em") > F.lit(AGORA), F.col("criado_em"), DETALHE_FUTURO),
    ("categoria_ausente_ou_fora_da_lista",
     F.col("categoria").isNull() | ~F.col("categoria").isin(*CATEGORIAS_RELATO),
     F.col("categoria"), F.lit("categoria nula ou fora de " + ", ".join(CATEGORIAS_RELATO))),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultados
# MAGIC Tire o print das quatro tabelas abaixo **depois do lote 1**, **depois do lote 2** e **depois de reexecutar sem arquivo novo** (nesse último caso nada muda).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Aceitos por fonte (linhas em `prata_<fonte>`)

# COMMAND ----------

aceitos_por_fonte = reduce(
    DataFrame.unionByName,
    [spark.table(prata(f)).agg(F.lit(f).alias("fonte"), F.count(F.lit(1)).alias("aceitos")) for f in FONTES],
)
display(aceitos_por_fonte)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Barrados por fonte e regra (`prata_rejeitados`)
# MAGIC Todas as combinações fonte × regra que se aplicam aparecem, inclusive com total 0.

# COMMAND ----------

grade = spark.createDataFrame([(f, r) for f, regras in REGRAS.items() for r in regras], "fonte string, regra string")
barrados = (
    grade.join(
        spark.table(TABELA_REJEITADOS).groupBy("fonte", "regra").agg(F.count(F.lit(1)).alias("total")),
        ["fonte", "regra"],
        "left",
    )
    .withColumn("total", F.coalesce(F.col("total"), F.lit(0)))
    .select("fonte", "regra", "total")
    .orderBy("fonte", "regra")
)
display(barrados)

# Regra que não barrou nada em nenhuma fonte: avisar, sem inventar dado ruim
for regra, soma in barrados.groupBy("regra").agg(F.sum("total").alias("soma")).collect():
    if soma == 0:
        print(f"ATENÇÃO: a regra '{regra}' deu zero em todas as fontes.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. `tem_coordenada` no Infosiga

# COMMAND ----------

display(
    spark.table(prata("infosiga")).groupBy("tem_coordenada").agg(F.count(F.lit(1)).alias("sinistros"))
    .orderBy(F.col("tem_coordenada").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Status do SP156

# COMMAND ----------

display(
    spark.table(prata("sp156")).groupBy("status").agg(F.count(F.lit(1)).alias("solicitacoes"))
    .orderBy(F.col("solicitacoes").desc())
)
