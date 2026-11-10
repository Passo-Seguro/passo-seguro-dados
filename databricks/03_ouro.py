# Databricks notebook source
# MAGIC %md
# MAGIC # Passo Seguro — 03 Ouro
# MAGIC
# MAGIC **Objetivo.** Ler a Prata e entregar um **modelo dimensional** pronto para consulta e para o painel: duas tabelas fato (`fato_ocorrencia`, `fato_sinistro_pedestre`) e três dimensões (`dim_tempo`, `dim_local`, `dim_categoria`). A Ouro **não tem dado pessoal**: sem número do endereço, sem CEP, sem id original e com coordenadas arredondadas.
# MAGIC
# MAGIC **Reconstruída inteira a cada execução.** A Ouro é derivada da Prata, então toda tabela é gravada com `CREATE OR REPLACE TABLE ... AS SELECT`. Não há `MERGE` aqui: rodar de novo (por exemplo depois do lote 2) refaz tudo a partir da Prata e o resultado é o mesmo para os mesmos dados. Ordem: Bronze → Prata → **Ouro**.
# MAGIC
# MAGIC ## Modelo
# MAGIC
# MAGIC | Tabela | Uma linha por | Colunas |
# MAGIC |---|---|---|
# MAGIC | `dim_tempo` | data | `data`, `ano`, `mes`, `dia_semana`, `precipitacao_dia_mm`, `chuva_forte` |
# MAGIC | `dim_local` | par (subprefeitura, distrito) | `local_id`, `subprefeitura`, `distrito` |
# MAGIC | `dim_categoria` | categoria de cada fonte | `categoria_id`, `categoria`, `grupo` |
# MAGIC | `fato_ocorrencia` | solicitação do SP156 **e** relato | `id_ocorrencia_hash`, `origem` (`sp156`/`relato`), `data`, `local_id`, `categoria_id`, `status`, `dias_ate_conclusao`, `simulado`, `latitude_arred`, `longitude_arred` |
# MAGIC | `fato_sinistro_pedestre` | sinistro do Infosiga com pedestre | `id_sinistro`, `data`, `local_id`, `qtd_pedestres`, `qtd_pedestres_feridos`, `qtd_pedestres_mortos` |
# MAGIC
# MAGIC As chaves estrangeiras são `data` → `dim_tempo`, `local_id` → `dim_local` e `categoria_id` → `dim_categoria`.
# MAGIC
# MAGIC ## Regras
# MAGIC
# MAGIC **Local (subprefeitura e distrito) por ponto no polígono.** Os limites vêm do GeoSampa (`chegada/geo/subprefeitura.geojson` e `distrito.geojson`, arquivos de referência, não lote). Cada ponto recebe a subprefeitura e o distrito do polígono que o contém, com `shapely`. Um ponto que cai numa fresta de poucos metros entre dois polígonos (efeito da simplificação) recebe o polígono mais próximo, até ~50 m. **Sem coordenada, ou fora de qualquer polígono, o local é `NAO_IDENTIFICADO`** (no lote 1: 71 sinistros sem coordenada e 7 registros com coordenada fora dos limites; a seção 2 mostra quais e a que distância ficam). Vale para as três fontes, inclusive o SP156: a subprefeitura que o SP156 declara (`prefeitura_operacional`) é comparada na seção abaixo e **não é confiável** (valores que não são subprefeitura, e ~2% que contradizem o próprio distrito ou o ponto), então ela só serve de reserva se o ponto não cair em nenhum polígono. Os nomes do GeoSampa e do SP156 já coincidem (maiúsculas, sem acento), por isso não há mapa de correção.
# MAGIC
# MAGIC **Status.** SP156: `em_aberto`, `finalizada`, `cancelada` (a fonte não tem "indeferida"). Relatos (status do contrato da API): `novo`, `revisao_humana`, `validado` e `encaminhado` → `em_aberto`; `resolvido` → `finalizada`; `rejeitado` → `indeferida`. `dias_ate_conclusao` = dias entre abertura e finalização, **só das `finalizada` do SP156**; relatos não têm data de conclusão, então ficam nulos.
# MAGIC
# MAGIC **Chuva forte.** `precipitacao_dia_mm` é a soma das 24 horas da `prata_open_meteo` no dia (fuso de São Paulo). `chuva_forte = precipitacao_dia_mm >= 10 mm/dia`; dia sem clima fica com os dois campos nulos (desconhecido, não "sem chuva"). **Por que 10 mm:** é um limiar analítico, escolhido pelos dados. Com 50 mm/dia (o patamar de alerta de chuva intensa do INMET) nenhum dia de junho a agosto de 2026 qualifica e a comparação seria impossível; com 10 mm há 4 dias no lote 1 (de 92) e 23 de 161 no clima até 08/11, o suficiente para comparar e ainda assim dias claramente chuvosos. A amostra do lote 1 é pequena (4 dias), então a Q5 deve ser lida como indício. O clima é de um ponto só, na Praça da Sé, e vale para a cidade inteira.
# MAGIC
# MAGIC ## LGPD
# MAGIC
# MAGIC A Prata é a camada interna; a Ouro é a que vai para consulta, painel e API. Por isso a Ouro só leva o que é necessário para agregar por local, tempo e categoria:
# MAGIC
# MAGIC | Dado | Tratamento na Ouro | Por quê |
# MAGIC |---|---|---|
# MAGIC | Número do endereço e CEP (SP156) | **não entram**; ficam só na Prata | Com a rua, identificam o imóvel e, portanto, o seu dono ou morador, que é quem fez o pedido |
# MAGIC | Coordenadas | arredondadas a 3 casas (~100 m): `latitude_arred`, `longitude_arred` | Basta para mapa e agregação; 100 m já não aponta uma casa. A subprefeitura e o distrito são atribuídos antes, com a coordenada completa |
# MAGIC | `id` do relato (e `id_solicitacao` do SP156) | `id_ocorrencia_hash = sha2(concat(sal, id), 256)`; o id original **não entra** | **Pseudonimização.** O `id` do relato é a chave que liga o registro à foto e à descrição guardadas no banco do app. Com o sal, que é secreto, ninguém com acesso só à Ouro consegue refazer essa ligação nem testar ids conhecidos. O sal é lido de `/Volumes/workspace/gs/privado/sal.txt` e **nunca é exibido nem gravado**. No SP156 o hash também impede refazer o endereço a partir do `id_solicitacao`, que é derivado dele |
# MAGIC | Texto livre e foto (`descricao`, `foto_url`), `logradouro` | **não entram** | O cidadão pode digitar telefone, CPF ou nome na descrição. Como a análise não usa o texto, ele fica na Prata em vez de ser mascarado na Ouro |
# MAGIC
# MAGIC **Não existe identificador do cidadão nos relatos hoje.** O contrato da API nunca devolve o `autor_id` (a API guarda só o hash), e o JSON coletado, a Bronze e a Prata não têm esse campo; o único identificador é o `id` do próprio relato, que é o que recebe o hash. **Se a API passar a enviar um identificador do cidadão, ele recebe o mesmo hash (`sha2(concat(sal, autor), 256)`) numa coluna própria**, nunca em `id_ocorrencia_hash`. A última célula mostra 5 relatos e prova que nenhuma coluna da Ouro contém número, CEP, id original ou texto livre.
# MAGIC
# MAGIC **Análises usam só o SP156.** Os relatos são **simulados** (`simulado = true`) e existem só nas áreas-piloto; entram na `fato_ocorrencia` para o painel, mas `04_consultas` e a `vw_prioridades` filtram `origem = 'sp156'`.

# COMMAND ----------

# MAGIC %pip install -q "shapely>=2.0,<3"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import re
import unicodedata

import shapely
from pyspark.sql import Window
from pyspark.sql import functions as F
from shapely.geometry import shape

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

CATALOGO = "workspace"
SCHEMA = "gs"
VOLUME_GEO = "/Volumes/workspace/gs/chegada/geo"
ARQUIVO_SAL = "/Volumes/workspace/gs/privado/sal.txt"

LIMIAR_CHUVA_FORTE_MM = 10.0  # mm/dia; justificativa no markdown do topo
NAO_IDENTIFICADO = "NAO_IDENTIFICADO"
DISTANCIA_MAX_GRAUS = 50 / 111_000  # ~50 m: ponto numa fresta entre polígonos vale o vizinho mais próximo

# Status do relato (contrato da API) -> status da Ouro
MAPA_STATUS_RELATO = {
    "novo": "em_aberto",
    "revisao_humana": "em_aberto",
    "validado": "em_aberto",
    "encaminhado": "em_aberto",
    "resolvido": "finalizada",
    "rejeitado": "indeferida",
}

# Categorias: (origem, categoria na Prata, grupo). O grupo 'calcada' reúne SP156 calçada e as 4 classes dos relatos
CATEGORIAS = [
    (1, "calcada", "calcada"),
    (2, "guia_sarjeta", "guia_sarjeta"),
    (3, "buraco", "calcada"),
    (4, "piso_danificado", "calcada"),
    (5, "obstaculo", "calcada"),
    (6, "veiculo_na_calcada", "calcada"),
]


def t(nome):
    return f"{CATALOGO}.{SCHEMA}.{nome}"


def substituir(tabela, df):
    """A Ouro é derivada: CREATE OR REPLACE TABLE ... AS SELECT, sempre inteira."""
    df.createOrReplaceTempView("_origem_ouro")
    spark.sql(f"CREATE OR REPLACE TABLE {t(tabela)} AS SELECT * FROM _origem_ouro")
    print(f"[{tabela}] {spark.table(t(tabela)).count()} linhas")


# Sal: lido do Volume privado e usado só dentro dos hashes. Não é impresso nem gravado.
with open(ARQUIVO_SAL, encoding="utf-8") as f:
    SAL = f.read().strip()
assert re.fullmatch(r"[0-9a-f]{64}", SAL), "sal.txt fora do formato esperado (64 caracteres hex)"
print("Sal lido (64 caracteres hex). O valor não é exibido.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Limites geográficos
# MAGIC Lê os dois GeoJSON do GeoSampa e confere as contagens (32 subprefeituras, 96 distritos).

# COMMAND ----------

def normalizar(s):
    """Maiúsculas, sem acento, espaços simples."""
    if s is None:
        return None
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().upper()
    return re.sub(r"\s+", " ", s).strip()


def carregar_camada(arquivo):
    with open(f"{VOLUME_GEO}/{arquivo}", encoding="utf-8") as f:
        return json.load(f)["features"]


sub_feicoes = carregar_camada("subprefeitura.geojson")
dis_feicoes = carregar_camada("distrito.geojson")
assert len(sub_feicoes) == 32, f"esperava 32 subprefeituras, vieram {len(sub_feicoes)}"
assert len(dis_feicoes) == 96, f"esperava 96 distritos, vieram {len(dis_feicoes)}"

sub_nomes = [normalizar(f["properties"]["nm_subprefeitura"]) for f in sub_feicoes]
dis_nomes = [normalizar(f["properties"]["nm_distrito_municipal"]) for f in dis_feicoes]
sub_geoms = [shape(f["geometry"]) for f in sub_feicoes]
arvore_sub = shapely.STRtree(sub_geoms)
arvore_dis = shapely.STRtree([shape(f["geometry"]) for f in dis_feicoes])

# Cada distrito pertence a uma subprefeitura: base da dim_local
sub_por_codigo = {f["properties"]["cd_identificador_subprefeitura"]: n for f, n in zip(sub_feicoes, sub_nomes)}
referencia_local = [
    (sub_por_codigo[f["properties"]["cd_identificador_subprefeitura"]], n) for f, n in zip(dis_feicoes, dis_nomes)
]
print(f"{len(sub_nomes)} subprefeituras, {len(dis_nomes)} distritos")


def achar(arvore, nomes, lons, lats):
    """Para cada ponto: o polígono que o contém; senão o mais próximo até DISTANCIA_MAX_GRAUS; senão None."""
    pontos = shapely.points(lons, lats)
    achado = [None] * len(pontos)
    for i, j in zip(*arvore.query(pontos, predicate="within")):
        if achado[i] is None or j < achado[i]:  # borda compartilhada: o menor índice, para ser determinístico
            achado[i] = int(j)
    faltam = [i for i, a in enumerate(achado) if a is None]
    if faltam:
        for k, j in zip(*arvore.query_nearest(pontos[faltam], max_distance=DISTANCIA_MAX_GRAUS, all_matches=False)):
            achado[faltam[k]] = int(j)
    return [nomes[a] if a is not None else None for a in achado]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Atribuição de subprefeitura e distrito por ponto
# MAGIC Os pontos distintos das três fontes (SP156, Infosiga com coordenada, relatos) são resolvidos no driver e voltam como uma tabela pequena `(lat, lon) → (subprefeitura, distrito)`, que as fontes juntam por igualdade exata. A atribuição usa a coordenada **completa**; o arredondamento só vem depois, na gravação.

# COMMAND ----------

pontos = (
    spark.table(t("prata_sp156")).select(F.col("latitude").alias("lat"), F.col("longitude").alias("lon"))
    .unionByName(
        spark.table(t("prata_infosiga")).filter("tem_coordenada").select(F.col("latitude").alias("lat"), F.col("longitude").alias("lon"))
    )
    .unionByName(spark.table(t("prata_relatos")).select("lat", "lon"))
    .filter("lat IS NOT NULL AND lon IS NOT NULL")
    .distinct()
    .toPandas()
)
lats = pontos["lat"].to_numpy(dtype=float)
lons = pontos["lon"].to_numpy(dtype=float)

subs = achar(arvore_sub, sub_nomes, lons, lats)
dists = achar(arvore_dis, dis_nomes, lons, lats)
mapa_geo = spark.createDataFrame(
    list(zip(lats.tolist(), lons.tolist(), subs, dists)),
    "lat double, lon double, subprefeitura_geo string, distrito_geo string",
)
mapa_geo.createOrReplaceTempView("_mapa_geo")
resumo_mapa = spark.sql("""
    SELECT COUNT(*) AS pontos_distintos,
           SUM(CASE WHEN subprefeitura_geo IS NULL THEN 1 ELSE 0 END) AS sem_subprefeitura,
           SUM(CASE WHEN distrito_geo IS NULL THEN 1 ELSE 0 END) AS sem_distrito
    FROM _mapa_geo
""")
display(resumo_mapa)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pontos com coordenada que não caem em nenhum polígono
# MAGIC Além dos sinistros **sem coordenada** (71 no lote 1, que ficam `NAO_IDENTIFICADO` por definição), alguns pontos com coordenada ficam fora de todo polígono. No lote 1 são 6 pontos distintos (7 registros: 6 sinistros do Infosiga e 1 solicitação do SP156). **Não é efeito da simplificação:** conferido no GeoJSON original do GeoSampa (sem simplificar), esses pontos também ficam fora de todos os polígonos, e a distância ao polígono mais próximo muda no máximo ~2 m entre o original e o simplificado. Estão **fora dos limites do município** na geometria oficial: a menos de ~160 m dos limites (provável ponto de divisa ou imprecisão do GPS na via) e dois a 1,3 km e 5,5 km, bem além da divisa (coordenada provavelmente errada no Infosiga, que informa município São Paulo). A tolerância de ~50 m cobre só as frestas da simplificação; **não a aumento**, porque atribuir a uma subprefeitura um ponto que está fora do município seria chutar. Por isso, a geometria original não é usada na atribuição. Os pontos aparecem abaixo, com a coordenada arredondada a 3 casas, e o total por origem na seção 6.

# COMMAND ----------

sem_poligono = [i for i, s in enumerate(subs) if s is None or dists[i] is None]
distancias = []
for i in sem_poligono:
    ponto = shapely.Point(float(lons[i]), float(lats[i]))
    distancias.append(min(g.distance(ponto) for g in sub_geoms) * 111_000)  # graus -> metros (aprox.)
display(
    spark.createDataFrame(
        [(round(float(lats[i]), 3), round(float(lons[i]), 3), round(d)) for i, d in zip(sem_poligono, distancias)],
        "lat_arred double, lon_arred double, distancia_ao_poligono_mais_proximo_m long",
    ).orderBy("distancia_ao_poligono_mais_proximo_m")
)


def com_local(df, col_lat, col_lon):
    """Junta subprefeitura_geo e distrito_geo pelo ponto (igualdade exata, mesma origem dos valores)."""
    g = mapa_geo.withColumnRenamed("lat", "_g_lat").withColumnRenamed("lon", "_g_lon")
    return df.join(F.broadcast(g), (df[col_lat] == g["_g_lat"]) & (df[col_lon] == g["_g_lon"]), "left").drop("_g_lat", "_g_lon")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Subprefeitura declarada no SP156 × GeoSampa
# MAGIC Compara `prata_sp156.prefeitura_operacional` e `distrito` com os nomes do GeoSampa (normalizados). Os valores abaixo **não são subprefeitura** e, por isso, o SP156 não é usado como fonte do local.

# COMMAND ----------

ACENTOS = "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ"
SEM_ACENTO = "AAAAAEEEEIIIIOOOOOUUUUCN"


def normalizar_col(c):
    """Mesma regra de normalizar(): maiúsculas, sem acento."""
    return F.trim(F.translate(F.upper(c), ACENTOS, SEM_ACENTO))


declaradas = spark.table(t("prata_sp156")).select(
    normalizar_col(F.col("prefeitura_operacional")).alias("sub_declarada"),
    normalizar_col(F.col("distrito")).alias("dis_declarado"),
    "latitude", "longitude",
)

print("Valores de prefeitura_operacional que não são uma das 32 subprefeituras do GeoSampa:")
display(
    declaradas.filter(~F.col("sub_declarada").isin(*sub_nomes) | F.col("sub_declarada").isNull())
    .groupBy("sub_declarada").count().orderBy(F.col("count").desc())
)

comparacao = com_local(declaradas, "latitude", "longitude").agg(
    F.count(F.lit(1)).alias("linhas"),
    F.sum(F.when(F.col("sub_declarada").isin(*sub_nomes), 1).otherwise(0)).alias("sub_declarada_valida"),
    F.sum(F.when(F.col("sub_declarada").isin(*sub_nomes) & (F.col("sub_declarada") != F.col("subprefeitura_geo")), 1).otherwise(0)).alias("diverge_do_ponto"),
    F.sum(F.when(F.col("dis_declarado").isin(*dis_nomes) & (F.col("dis_declarado") != F.col("distrito_geo")), 1).otherwise(0)).alias("distrito_diverge_do_ponto"),
)
display(comparacao)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bases por fonte (com local e hash)

# COMMAND ----------

SUBPREFEITURAS = sub_nomes
DISTRITOS = dis_nomes


def hash_id(coluna):
    """sha2(concat(sal, id), 256): o id original não segue adiante."""
    return F.sha2(F.concat(F.lit(SAL), F.col(coluna).cast("string")), 256)


def mapear(coluna, mapa):
    expressao = None
    for origem, destino in mapa.items():
        condicao = F.col(coluna) == origem
        expressao = F.when(condicao, destino) if expressao is None else expressao.when(condicao, destino)
    return expressao


# SP156: o ponto no polígono manda; o declarado (se for um nome válido) só entra quando o ponto não cai em polígono
sub_decl = normalizar_col(F.col("prefeitura_operacional"))
dis_decl = normalizar_col(F.col("distrito"))

base_sp156 = (
    com_local(spark.table(t("prata_sp156")), "latitude", "longitude")
    .select(
        hash_id("id_solicitacao").alias("id_ocorrencia_hash"),
        F.lit("sp156").alias("origem"),
        F.col("data_abertura").alias("data"),
        F.coalesce(F.col("subprefeitura_geo"), F.when(sub_decl.isin(*SUBPREFEITURAS), sub_decl), F.lit(NAO_IDENTIFICADO)).alias("subprefeitura"),
        F.coalesce(F.col("distrito_geo"), F.when(dis_decl.isin(*DISTRITOS), dis_decl), F.lit(NAO_IDENTIFICADO)).alias("distrito"),
        F.col("categoria"),
        F.col("status"),
        F.when((F.col("status") == "finalizada") & F.col("data_finalizacao").isNotNull(), F.datediff("data_finalizacao", "data_abertura"))
        .cast("int").alias("dias_ate_conclusao"),
        F.lit(False).alias("simulado"),
        F.round("latitude", 3).alias("latitude_arred"),
        F.round("longitude", 3).alias("longitude_arred"),
    )
)

base_relatos = (
    com_local(spark.table(t("prata_relatos")), "lat", "lon")
    .select(
        hash_id("id").alias("id_ocorrencia_hash"),
        F.lit("relato").alias("origem"),
        F.to_date("criado_em").alias("data"),
        F.coalesce(F.col("subprefeitura_geo"), F.lit(NAO_IDENTIFICADO)).alias("subprefeitura"),
        F.coalesce(F.col("distrito_geo"), F.lit(NAO_IDENTIFICADO)).alias("distrito"),
        F.col("categoria"),
        mapear("status", MAPA_STATUS_RELATO).alias("status"),
        F.lit(None).cast("int").alias("dias_ate_conclusao"),
        F.coalesce(F.col("simulado"), F.lit(False)).alias("simulado"),
        F.round("lat", 3).alias("latitude_arred"),
        F.round("lon", 3).alias("longitude_arred"),
    )
)

base_sinistros = (
    com_local(spark.table(t("prata_infosiga")), "latitude", "longitude")
    .select(
        F.col("id_sinistro"),
        F.col("data_sinistro").alias("data"),
        F.coalesce(F.col("subprefeitura_geo"), F.lit(NAO_IDENTIFICADO)).alias("subprefeitura"),
        F.coalesce(F.col("distrito_geo"), F.lit(NAO_IDENTIFICADO)).alias("distrito"),
        "qtd_pedestres", "qtd_pedestres_feridos", "qtd_pedestres_mortos",
    )
)

base_ocorrencia = base_sp156.unionByName(base_relatos)

# Relato com status fora do mapa ficaria nulo: avisar (o contrato da API tem só os 6 acima)
sem_status = spark.table(t("prata_relatos")).filter("status IS NOT NULL").select("status").distinct().filter(~F.col("status").isin(*MAPA_STATUS_RELATO)).collect()
if sem_status:
    print("ATENÇÃO: status de relato sem mapeamento (ficam nulos):", [r["status"] for r in sem_status])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dimensões

# COMMAND ----------

# dim_tempo: uma linha por data, do primeiro ao último dia de qualquer fonte (inclusive o clima)
clima_diario = (
    spark.table(t("prata_open_meteo"))
    .groupBy(F.to_date("data_hora").alias("data"))
    .agg(F.round(F.sum("precipitacao_mm"), 1).alias("precipitacao_dia_mm"))
)

todas_as_datas = (
    base_ocorrencia.select("data")
    .unionByName(base_sinistros.select("data"))
    .unionByName(clima_diario.select("data"))
    .filter(F.col("data").isNotNull())
)

dim_tempo = (
    todas_as_datas.agg(F.min("data").alias("inicio"), F.max("data").alias("fim"))
    .select(F.explode(F.sequence("inicio", "fim", F.expr("INTERVAL 1 DAY"))).alias("data"))
    .join(clima_diario, "data", "left")
    .select(
        "data",
        F.year("data").alias("ano"),
        F.month("data").alias("mes"),
        F.expr("elt(dayofweek(data), 'domingo', 'segunda', 'terca', 'quarta', 'quinta', 'sexta', 'sabado')").alias("dia_semana"),
        "precipitacao_dia_mm",
        F.when(F.col("precipitacao_dia_mm").isNotNull(), F.col("precipitacao_dia_mm") >= LIMIAR_CHUVA_FORTE_MM).alias("chuva_forte"),
    )
)
substituir("dim_tempo", dim_tempo)

# dim_local: os 96 distritos do GeoSampa com a sua subprefeitura + um "NAO_IDENTIFICADO" por subprefeitura + o total desconhecido
# + qualquer par observado que fuja disso (ex.: ponto de borda), para toda chave estrangeira ter dimensão
locais_referencia = spark.createDataFrame(referencia_local, "subprefeitura string, distrito string")
locais_sem_distrito = spark.createDataFrame(
    [(s, NAO_IDENTIFICADO) for s in SUBPREFEITURAS] + [(NAO_IDENTIFICADO, NAO_IDENTIFICADO)], "subprefeitura string, distrito string"
)
locais_observados = (
    base_ocorrencia.select("subprefeitura", "distrito").unionByName(base_sinistros.select("subprefeitura", "distrito"))
)
dim_local = (
    locais_referencia.unionByName(locais_sem_distrito).unionByName(locais_observados).distinct()
    .withColumn("local_id", F.row_number().over(Window.orderBy("subprefeitura", "distrito")))
    .select("local_id", "subprefeitura", "distrito")
)
substituir("dim_local", dim_local)

# dim_categoria
dim_categoria = spark.createDataFrame(CATEGORIAS, "categoria_id int, categoria string, grupo string")
substituir("dim_categoria", dim_categoria)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fatos

# COMMAND ----------

dim_local_t = spark.table(t("dim_local"))
dim_categoria_t = spark.table(t("dim_categoria")).select("categoria_id", "categoria")

fato_ocorrencia = (
    base_ocorrencia
    .join(dim_local_t, ["subprefeitura", "distrito"], "left")
    .join(dim_categoria_t, "categoria", "left")
    .select(
        "id_ocorrencia_hash", "origem", "data", "local_id", "categoria_id", "status",
        "dias_ate_conclusao", "simulado", "latitude_arred", "longitude_arred",
    )
)
substituir("fato_ocorrencia", fato_ocorrencia)

fato_sinistro_pedestre = (
    base_sinistros
    .join(dim_local_t, ["subprefeitura", "distrito"], "left")
    .select("id_sinistro", "data", "local_id", "qtd_pedestres", "qtd_pedestres_feridos", "qtd_pedestres_mortos")
)
substituir("fato_sinistro_pedestre", fato_sinistro_pedestre)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Conferências
# MAGIC Cada fato tem de ter as mesmas linhas da Prata de onde veio, nenhuma chave estrangeira solta e chave única.

# COMMAND ----------

checagens = []


def checar(nome, ok, detalhe=""):
    checagens.append((nome, bool(ok), str(detalhe)))


def contar(tabela, filtro=None):
    df = spark.table(t(tabela))
    return (df.filter(filtro) if filtro else df).count()


n_sp156 = contar("fato_ocorrencia", "origem = 'sp156'")
n_relato = contar("fato_ocorrencia", "origem = 'relato'")
n_sinistro = contar("fato_sinistro_pedestre")
checar("fato_ocorrencia (sp156) = prata_sp156", n_sp156 == contar("prata_sp156"), f"{n_sp156} linhas")
checar("fato_ocorrencia (relato) = prata_relatos", n_relato == contar("prata_relatos"), f"{n_relato} linhas")
checar("fato_sinistro_pedestre = prata_infosiga", n_sinistro == contar("prata_infosiga"), f"{n_sinistro} linhas")
checar("id_ocorrencia_hash é único", contar("fato_ocorrencia") == spark.table(t("fato_ocorrencia")).select("id_ocorrencia_hash").distinct().count())
checar("id_sinistro é único", contar("fato_sinistro_pedestre") == spark.table(t("fato_sinistro_pedestre")).select("id_sinistro").distinct().count())
n_datas = contar("dim_tempo")
checar("dim_tempo: uma linha por data", n_datas == spark.table(t("dim_tempo")).select("data").distinct().count(), f"{n_datas} datas")
checar("dim_local: as 32 subprefeituras", spark.table(t("dim_local")).filter(f"subprefeitura <> '{NAO_IDENTIFICADO}'").select("subprefeitura").distinct().count() == 32)
for fato in ("fato_ocorrencia", "fato_sinistro_pedestre"):
    checar(f"{fato}: local_id sem dimensão", spark.table(t(fato)).join(spark.table(t("dim_local")), "local_id", "left_anti").count() == 0)
    checar(f"{fato}: data sem dim_tempo", spark.table(t(fato)).join(spark.table(t("dim_tempo")), "data", "left_anti").count() == 0)
checar("fato_ocorrencia: categoria_id sem dimensão", spark.table(t("fato_ocorrencia")).join(spark.table(t("dim_categoria")), "categoria_id", "left_anti").count() == 0)

display(spark.createDataFrame(checagens, "verificacao string, ok boolean, detalhe string"))
assert all(ok for _, ok, _ in checagens), "alguma conferência falhou (veja a tabela acima)"

# Registros sem local: nada se perde, só fica como NAO_IDENTIFICADO
display(
    spark.table(t("fato_ocorrencia")).join(spark.table(t("dim_local")), "local_id")
    .filter(f"subprefeitura = '{NAO_IDENTIFICADO}'").groupBy("origem").count()
    .unionByName(
        spark.table(t("fato_sinistro_pedestre")).join(spark.table(t("dim_local")), "local_id")
        .filter(f"subprefeitura = '{NAO_IDENTIFICADO}'").groupBy(F.lit("infosiga").alias("origem")).count()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. View `workspace.gs.vw_prioridades`
# MAGIC Uma linha por subprefeitura (as 32; `NAO_IDENTIFICADO` fica de fora), lida só da Ouro. Tem **duas famílias de colunas**:
# MAGIC
# MAGIC **a) Contrato de `GET /painel`** (`PrioridadeSubprefeitura` no `openapi.yaml`; nomes e tipos são os do contrato e é só isto que o `05_export` grava hoje):
# MAGIC
# MAGIC | Coluna | Tipo | Como é calculada |
# MAGIC |---|---|---|
# MAGIC | `subprefeitura` | string | nome do GeoSampa (maiúsculas, sem acento) |
# MAGIC | `prioridade` | string `alta`/`media`/`baixa` | faixa do `indice_prioridade` (abaixo) |
# MAGIC | `relatos_abertos` | int | relatos com status `em_aberto` (nem resolvidos nem rejeitados) |
# MAGIC | `relatos_por_categoria` | struct de 4 ints | os mesmos relatos abertos por `buraco`, `piso_danificado`, `obstaculo`, `veiculo_na_calcada` |
# MAGIC | `tempo_medio_ate_reparo_dias` | double (nulo sem reparos) | média de `dias_ate_conclusao` das solicitações **SP156 finalizadas** |
# MAGIC | `atualizado_em` | timestamp | `current_timestamp()` no momento da leitura; no `painel.json` é o instante da exportação |
# MAGIC
# MAGIC **b) Análise (colunas extras, ainda fora do contrato; ver `docs/proposta-contrato-painel.md`):** `reclamacoes_calcada_sp156`, `reclamacoes_guia_sarjeta_sp156`, `atropelamentos_pedestres` (sinistros do Infosiga com pedestre), `pedestres_mortos`, `indice_prioridade` e `pct_em_aberto` (% das solicitações SP156 da subprefeitura que estão `em_aberto`).
# MAGIC
# MAGIC **`indice_prioridade` (0 a 1)** = média de dois indicadores normalizados entre as 32 subprefeituras:
# MAGIC
# MAGIC `indice = ( mm(atropelamentos_pedestres) + mm(reclamacoes_calcada_sp156) ) / 2`, com `mm(x) = (x − mínimo) / (máximo − mínimo)` (0 se máximo = mínimo).
# MAGIC
# MAGIC Cada indicador pesa metade; guia e sarjeta ficam fora do índice (aparecem à parte), e o período é todo o carregado. É um índice relativo e propositalmente simples: 1 é a subprefeitura que lidera os dois, 0 a que está no piso dos dois.
# MAGIC
# MAGIC **Faixas de `prioridade`.** Usa a posição relativa do índice entre as 32 (`PERCENT_RANK`): do terço de cima, `alta` (posição ≥ 2/3); do terço do meio, `media` (≥ 1/3); do terço de baixo, `baixa`. Empates ficam na mesma faixa. A faixa é **relativa**, então sempre há subprefeituras nas três; não mede risco absoluto.
# MAGIC
# MAGIC **Relatos são simulados** (só Sé e Pinheiros, mais algumas bordas), então `relatos_abertos` e `relatos_por_categoria` são zero na maior parte da cidade até a API entrar no ar.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {t('vw_prioridades')} AS
WITH subprefeituras AS (
  SELECT DISTINCT subprefeitura FROM {t('dim_local')} WHERE subprefeitura <> '{NAO_IDENTIFICADO}'
),
solicitacoes AS (
  SELECT
    l.subprefeitura,
    COUNT(CASE WHEN c.grupo = 'calcada' THEN 1 END)       AS reclamacoes_calcada,
    COUNT(CASE WHEN c.grupo = 'guia_sarjeta' THEN 1 END)  AS reclamacoes_guia_sarjeta,
    COUNT(*)                                              AS total,
    COUNT(CASE WHEN f.status = 'em_aberto' THEN 1 END)    AS em_aberto,
    AVG(CASE WHEN f.status = 'finalizada' THEN f.dias_ate_conclusao END) AS dias_medio
  FROM {t('fato_ocorrencia')} f
  JOIN {t('dim_local')} l     ON l.local_id = f.local_id
  JOIN {t('dim_categoria')} c ON c.categoria_id = f.categoria_id
  WHERE f.origem = 'sp156'
  GROUP BY l.subprefeitura
),
relatos AS (
  SELECT
    l.subprefeitura,
    COUNT(*)                                                    AS abertos,
    COUNT(CASE WHEN c.categoria = 'buraco' THEN 1 END)             AS buraco,
    COUNT(CASE WHEN c.categoria = 'piso_danificado' THEN 1 END)    AS piso_danificado,
    COUNT(CASE WHEN c.categoria = 'obstaculo' THEN 1 END)          AS obstaculo,
    COUNT(CASE WHEN c.categoria = 'veiculo_na_calcada' THEN 1 END) AS veiculo_na_calcada
  FROM {t('fato_ocorrencia')} f
  JOIN {t('dim_local')} l     ON l.local_id = f.local_id
  JOIN {t('dim_categoria')} c ON c.categoria_id = f.categoria_id
  WHERE f.origem = 'relato' AND f.status = 'em_aberto'
  GROUP BY l.subprefeitura
),
sinistros AS (
  SELECT l.subprefeitura, COUNT(*) AS atropelamentos, SUM(s.qtd_pedestres_mortos) AS mortos
  FROM {t('fato_sinistro_pedestre')} s
  JOIN {t('dim_local')} l ON l.local_id = s.local_id
  GROUP BY l.subprefeitura
),
base AS (
  SELECT
    s.subprefeitura,
    CAST(COALESCE(r.abertos, 0) AS INT)  AS relatos_abertos,
    named_struct(
      'buraco',             CAST(COALESCE(r.buraco, 0) AS INT),
      'piso_danificado',    CAST(COALESCE(r.piso_danificado, 0) AS INT),
      'obstaculo',          CAST(COALESCE(r.obstaculo, 0) AS INT),
      'veiculo_na_calcada', CAST(COALESCE(r.veiculo_na_calcada, 0) AS INT)
    ) AS relatos_por_categoria,
    CAST(ROUND(p.dias_medio, 1) AS DOUBLE)                                   AS tempo_medio_ate_reparo_dias,
    CAST(COALESCE(p.reclamacoes_calcada, 0) AS INT)                          AS reclamacoes_calcada_sp156,
    CAST(COALESCE(p.reclamacoes_guia_sarjeta, 0) AS INT)                     AS reclamacoes_guia_sarjeta_sp156,
    CAST(COALESCE(i.atropelamentos, 0) AS INT)                               AS atropelamentos_pedestres,
    CAST(COALESCE(i.mortos, 0) AS INT)                                       AS pedestres_mortos,
    CAST(ROUND(100.0 * COALESCE(p.em_aberto, 0) / NULLIF(p.total, 0), 1) AS DOUBLE) AS pct_em_aberto
  FROM subprefeituras s
  LEFT JOIN solicitacoes p ON p.subprefeitura = s.subprefeitura
  LEFT JOIN relatos r      ON r.subprefeitura = s.subprefeitura
  LEFT JOIN sinistros i    ON i.subprefeitura = s.subprefeitura
),
indice AS (
  SELECT
    *,
    (
      COALESCE((atropelamentos_pedestres - MIN(atropelamentos_pedestres) OVER ())
               / NULLIF(MAX(atropelamentos_pedestres) OVER () - MIN(atropelamentos_pedestres) OVER (), 0), 0)
      +
      COALESCE((reclamacoes_calcada_sp156 - MIN(reclamacoes_calcada_sp156) OVER ())
               / NULLIF(MAX(reclamacoes_calcada_sp156) OVER () - MIN(reclamacoes_calcada_sp156) OVER (), 0), 0)
    ) / 2 AS indice
  FROM base
),
faixas AS (
  SELECT *, PERCENT_RANK() OVER (ORDER BY indice) AS posicao_relativa FROM indice
)
SELECT
  subprefeitura,
  CASE WHEN posicao_relativa >= 2.0 / 3 THEN 'alta'
       WHEN posicao_relativa >= 1.0 / 3 THEN 'media'
       ELSE 'baixa' END                       AS prioridade,
  relatos_abertos,
  relatos_por_categoria,
  tempo_medio_ate_reparo_dias,
  current_timestamp()                         AS atualizado_em,
  reclamacoes_calcada_sp156,
  reclamacoes_guia_sarjeta_sp156,
  atropelamentos_pedestres,
  pedestres_mortos,
  CAST(ROUND(indice, 3) AS DOUBLE)            AS indice_prioridade,
  pct_em_aberto
FROM faixas
""")

display(spark.table(t("vw_prioridades")).orderBy(F.col("indice_prioridade").desc(), "subprefeitura"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. LGPD: o que a Ouro não pode conter
# MAGIC Cinco linhas de relatos com a coluna de hash e os testes. Os testes comparam **nomes de colunas** e **valores** da Ouro com o que existe na Prata (número, CEP, ids originais, descrição dos relatos) e garantem que **não há coluna de texto livre**: as únicas colunas de texto são categóricas ou o hash, e uma coluna de texto nova faz o teste falhar. O sal não é exibido.

# COMMAND ----------

TABELAS_OURO = ["dim_tempo", "dim_local", "dim_categoria", "fato_ocorrencia", "fato_sinistro_pedestre", "vw_prioridades"]

# 5 linhas de relatos: o id aparece só como hash
display(
    spark.table(t("fato_ocorrencia")).filter(F.col("origem") == "relato").orderBy("data", "id_ocorrencia_hash")
    .select("id_ocorrencia_hash", "origem", "data", "local_id", "categoria_id", "status", "simulado", "latitude_arred", "longitude_arred")
    .limit(5)
)

testes = []


def testar(nome, ok, detalhe=""):
    testes.append((nome, bool(ok), str(detalhe)))


# 1) Nomes de colunas: nada de número, CEP, logradouro, autor/cidadão, id original nem coordenada exata
PROIBIDAS = re.compile(r"(^|_)(numero|cep|logradouro|endereco|autor|cidadao|solicitacao)(_|$)|^id$|^lat$|^lon$|^latitude$|^longitude$")
colunas_proibidas = [
    f"{tabela}.{coluna}"
    for tabela in TABELAS_OURO
    for coluna in spark.table(t(tabela)).columns
    if PROIBIDAS.search(coluna.lower())
]
testar("nenhuma coluna da Ouro chama-se número, CEP, logradouro, id ou coordenada exata", not colunas_proibidas, colunas_proibidas or "ok")

# 2) Valores: o id original (relato.id, id_solicitacao) e o CEP não aparecem em nenhuma coluna de texto da Ouro
ids_originais = (
    spark.table(t("prata_relatos")).select(F.col("id").cast("string").alias("v"))
    .unionByName(spark.table(t("prata_sp156")).select(F.col("id_solicitacao").cast("string").alias("v")))
    .distinct()
)
ceps = spark.table(t("prata_sp156")).filter("cep IS NOT NULL").select(F.col("cep").cast("string").alias("v")).distinct()

achados_id, achados_cep = [], []
for tabela in TABELAS_OURO:
    df = spark.table(t(tabela))
    for coluna, tipo in df.dtypes:
        if tipo != "string":
            continue
        valores = df.select(F.col(coluna).alias("c"))
        if valores.join(ids_originais, valores["c"] == ids_originais["v"], "left_semi").limit(1).count():
            achados_id.append(f"{tabela}.{coluna}")
        if coluna != "id_sinistro":  # id_sinistro é o id público do Infosiga, que pode ter 8 dígitos como um CEP
            if valores.join(ceps, valores["c"] == ceps["v"], "left_semi").limit(1).count():
                achados_cep.append(f"{tabela}.{coluna}")
testar("nenhum id original (relato.id, id_solicitacao) em coluna de texto da Ouro", not achados_id, achados_id or "ok")
testar("nenhum CEP do SP156 em coluna de texto da Ouro", not achados_cep, achados_cep or "ok")

# 3) O hash é o pedido: sha2(concat(sal, id), 256) para os relatos; 64 hex
relatos_recalculados = spark.table(t("prata_relatos")).select(F.sha2(F.concat(F.lit(SAL), F.col("id")), 256).alias("h"))
fato = spark.table(t("fato_ocorrencia"))
hash_relatos = fato.filter(F.col("origem") == "relato")
n_relatos = hash_relatos.count()
conferem = hash_relatos.join(relatos_recalculados, hash_relatos["id_ocorrencia_hash"] == relatos_recalculados["h"], "left_semi").count()
testar("id_ocorrencia_hash dos relatos = sha2(concat(sal, id), 256)", conferem == n_relatos, f"{conferem}/{n_relatos}")
testar("todo id_ocorrencia_hash tem 64 caracteres hex", fato.filter(~F.col("id_ocorrencia_hash").rlike("^[0-9a-f]{64}$")).count() == 0)

# 4) Coordenadas só com até 3 casas decimais (~100 m)
mais_de_3_casas = fato.filter(
    ~F.col("latitude_arred").cast("string").rlike(r"^-?\d+(\.\d{1,3})?$")
    | ~F.col("longitude_arred").cast("string").rlike(r"^-?\d+(\.\d{1,3})?$")
).count()
testar("latitude_arred e longitude_arred têm no máximo 3 casas decimais", mais_de_3_casas == 0, f"{mais_de_3_casas} fora")

# 5) Nenhuma coluna de texto livre (descrição do relato, foto, logradouro): só as colunas de texto abaixo, todas
# categóricas ou hash. Qualquer coluna de texto nova na Ouro derruba o teste e obriga a revisar.
TEXTO_PERMITIDO = {
    "dim_tempo": {"dia_semana"},
    "dim_local": {"subprefeitura", "distrito"},
    "dim_categoria": {"categoria", "grupo"},
    "fato_ocorrencia": {"id_ocorrencia_hash", "origem", "status"},
    "fato_sinistro_pedestre": {"id_sinistro"},
    "vw_prioridades": {"subprefeitura", "prioridade"},
}
TEXTO_LIVRE = re.compile(r"descri|texto|observ|coment|mensagem|relato_txt|foto|url|logradouro|endereco")
colunas_de_texto = {tabela: {c for c, tipo in spark.table(t(tabela)).dtypes if tipo == "string"} for tabela in TABELAS_OURO}
inesperadas = {tabela: sorted(cols - TEXTO_PERMITIDO[tabela]) for tabela, cols in colunas_de_texto.items() if cols - TEXTO_PERMITIDO[tabela]}
nomes_de_texto_livre = [f"{tabela}.{c}" for tabela in TABELAS_OURO for c in spark.table(t(tabela)).columns if TEXTO_LIVRE.search(c.lower())]
testar("só as colunas de texto previstas (nenhuma de texto livre) na Ouro", not inesperadas, inesperadas or "ok")
testar("nenhum nome de coluna da Ouro indica texto livre, foto ou logradouro", not nomes_de_texto_livre, nomes_de_texto_livre or "ok")

# valores: nenhum texto livre dos relatos (descricao) aparece em coluna de texto, e nenhum valor passa de 64 caracteres (o hash)
descricoes = spark.table(t("prata_relatos")).filter("descricao IS NOT NULL").select(F.col("descricao").alias("v")).distinct()
achados_texto, mais_longos = [], 0
for tabela, cols in colunas_de_texto.items():
    df = spark.table(t(tabela))
    for coluna in cols:
        valores = df.select(F.col(coluna).alias("c"))
        if valores.join(descricoes, valores["c"] == descricoes["v"], "left_semi").limit(1).count():
            achados_texto.append(f"{tabela}.{coluna}")
        mais_longos += valores.filter(F.length("c") > 64).count()
testar("nenhuma descrição de relato aparece em coluna de texto da Ouro", not achados_texto, achados_texto or "ok")
testar("nenhum valor de texto da Ouro passa de 64 caracteres (tamanho do hash)", mais_longos == 0, f"{mais_longos} valores")

display(spark.createDataFrame(testes, "teste string, passou boolean, detalhe string"))
assert all(ok for _, ok, _ in testes), "TESTE DE LGPD FALHOU (veja a tabela acima)"
print("LGPD ok: nenhuma coluna da Ouro contém número, CEP, id original ou texto livre.")
