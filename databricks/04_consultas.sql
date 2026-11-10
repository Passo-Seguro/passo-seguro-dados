-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Passo Seguro — 04 Consultas
-- MAGIC
-- MAGIC Cinco perguntas sobre a **Ouro** (`workspace.gs`). Todas as consultas leem só `fato_*` e `dim_*`, nunca a Prata. Rode depois da `03_ouro`; depois do lote 2, rode `03_ouro` de novo e depois este notebook.
-- MAGIC
-- MAGIC **Escopo.** As consultas usam só `origem = 'sp156'`. Os relatos são **simulados** e existem só nas áreas-piloto (Sé e Pinheiros), então não entram em nenhuma comparação (a Q3 mostra as duas origens, separadas, de propósito).
-- MAGIC
-- MAGIC **Local.** Subprefeitura por ponto no polígono (GeoSampa). `NAO_IDENTIFICADO` (sem coordenada ou fora dos polígonos) fica fora dos rankings das subprefeituras.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q1 — Quais subprefeituras concentram mais reclamações de calçada?
-- MAGIC
-- MAGIC Duas leituras lado a lado: só **calçada** (`posicao_sem_guia`) e **calçada + guia e sarjeta** (`posicao_com_guia`). Aparecem as subprefeituras que estão entre as 10 primeiras em qualquer uma das duas.
-- MAGIC
-- MAGIC **Interpretação.**
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

WITH subprefeituras AS (
  SELECT DISTINCT subprefeitura
  FROM workspace.gs.dim_local
  WHERE subprefeitura <> 'NAO_IDENTIFICADO'
),
reclamacoes AS (
  SELECT
    l.subprefeitura,
    COUNT(CASE WHEN c.grupo = 'calcada' THEN 1 END)      AS reclamacoes_calcada,
    COUNT(CASE WHEN c.grupo = 'guia_sarjeta' THEN 1 END) AS reclamacoes_guia_sarjeta
  FROM workspace.gs.fato_ocorrencia f
  JOIN workspace.gs.dim_local l      ON l.local_id = f.local_id
  JOIN workspace.gs.dim_categoria c  ON c.categoria_id = f.categoria_id
  WHERE f.origem = 'sp156'
  GROUP BY l.subprefeitura
),
base AS (
  SELECT
    s.subprefeitura,
    COALESCE(r.reclamacoes_calcada, 0)      AS reclamacoes_calcada,
    COALESCE(r.reclamacoes_guia_sarjeta, 0) AS reclamacoes_guia_sarjeta
  FROM subprefeituras s
  LEFT JOIN reclamacoes r ON r.subprefeitura = s.subprefeitura
),
ranque AS (
  SELECT
    subprefeitura,
    reclamacoes_calcada,
    reclamacoes_guia_sarjeta,
    reclamacoes_calcada + reclamacoes_guia_sarjeta AS reclamacoes_calcada_e_guia,
    RANK() OVER (ORDER BY reclamacoes_calcada DESC)                              AS posicao_sem_guia,
    RANK() OVER (ORDER BY reclamacoes_calcada + reclamacoes_guia_sarjeta DESC)   AS posicao_com_guia
  FROM base
)
SELECT *
FROM ranque
WHERE posicao_sem_guia <= 10 OR posicao_com_guia <= 10
ORDER BY posicao_sem_guia, subprefeitura

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q2 — Quanto tempo leva, em média e na mediana, para uma solicitação ser concluída?
-- MAGIC
-- MAGIC Só solicitações **finalizadas**, em dias entre a abertura e a finalização, por categoria. As ainda abertas não entram (o tempo delas é desconhecido), então o valor tende a ficar abaixo do real, sobretudo nas solicitações mais recentes.
-- MAGIC
-- MAGIC **Interpretação.**
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT
  c.categoria,
  COUNT(*)                                  AS finalizadas,
  ROUND(AVG(f.dias_ate_conclusao), 1)       AS media_dias,
  ROUND(MEDIAN(f.dias_ate_conclusao), 1)    AS mediana_dias
FROM workspace.gs.fato_ocorrencia f
JOIN workspace.gs.dim_categoria c ON c.categoria_id = f.categoria_id
WHERE f.origem = 'sp156'
  AND f.status = 'finalizada'
  AND f.dias_ate_conclusao IS NOT NULL
GROUP BY c.categoria
ORDER BY c.categoria

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q3 — Qual a proporção de solicitações finalizadas, indeferidas, canceladas e em aberto?
-- MAGIC
-- MAGIC Por origem. O SP156 não tem "indeferida" (a linha aparece com zero de propósito); nos relatos simulados, "indeferida" é o status `rejeitado` e "cancelada" não existe.
-- MAGIC
-- MAGIC **Interpretação.**
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

WITH status_ouro AS (
  SELECT * FROM (VALUES (1, 'finalizada'), (2, 'indeferida'), (3, 'cancelada'), (4, 'em_aberto')) AS s(ordem, situacao)
),
origens AS (
  SELECT DISTINCT origem FROM workspace.gs.fato_ocorrencia
),
contagem AS (
  SELECT origem, status AS situacao, COUNT(*) AS n
  FROM workspace.gs.fato_ocorrencia
  GROUP BY origem, status
)
SELECT
  o.origem,
  s.situacao                                                              AS status,
  COALESCE(c.n, 0)                                                        AS solicitacoes,
  ROUND(100.0 * COALESCE(c.n, 0) / SUM(COALESCE(c.n, 0)) OVER (PARTITION BY o.origem), 1) AS percentual
FROM origens o
CROSS JOIN status_ouro s
LEFT JOIN contagem c ON c.origem = o.origem AND c.situacao = s.situacao
ORDER BY o.origem, s.ordem

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q4a — Onde atropelamentos de pedestres e reclamações de calçada se encontram? (só calçada)
-- MAGIC
-- MAGIC Por subprefeitura, de 01/06/2026 a 30/09/2026: `atropelamentos` = sinistros do Infosiga com pedestre; `reclamacoes` = solicitações SP156 de **calçada**. `quartil_superior_nas_duas` marca as subprefeituras com valor **maior ou igual ao percentil 75** (entre as 32 subprefeituras) nos dois indicadores. Sinistros sem coordenada ficam em `NAO_IDENTIFICADO` e não entram.
-- MAGIC
-- MAGIC **Interpretação.**
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

WITH subprefeituras AS (
  SELECT DISTINCT subprefeitura
  FROM workspace.gs.dim_local
  WHERE subprefeitura <> 'NAO_IDENTIFICADO'
),
atropelamentos AS (
  SELECT l.subprefeitura, COUNT(*) AS atropelamentos
  FROM workspace.gs.fato_sinistro_pedestre s
  JOIN workspace.gs.dim_local l ON l.local_id = s.local_id
  WHERE s.data BETWEEN DATE'2026-06-01' AND DATE'2026-09-30'
  GROUP BY l.subprefeitura
),
reclamacoes AS (
  SELECT l.subprefeitura, COUNT(*) AS reclamacoes
  FROM workspace.gs.fato_ocorrencia f
  JOIN workspace.gs.dim_local l      ON l.local_id = f.local_id
  JOIN workspace.gs.dim_categoria c  ON c.categoria_id = f.categoria_id
  WHERE f.origem = 'sp156'
    AND f.data BETWEEN DATE'2026-06-01' AND DATE'2026-09-30'
    AND c.grupo = 'calcada'
  GROUP BY l.subprefeitura
),
base AS (
  SELECT
    s.subprefeitura,
    COALESCE(a.atropelamentos, 0) AS atropelamentos,
    COALESCE(r.reclamacoes, 0)    AS reclamacoes
  FROM subprefeituras s
  LEFT JOIN atropelamentos a ON a.subprefeitura = s.subprefeitura
  LEFT JOIN reclamacoes r    ON r.subprefeitura = s.subprefeitura
),
limites AS (
  SELECT
    percentile_cont(0.75) WITHIN GROUP (ORDER BY atropelamentos) AS p75_atropelamentos,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY reclamacoes)    AS p75_reclamacoes
  FROM base
)
SELECT
  b.subprefeitura,
  b.atropelamentos,
  b.reclamacoes,
  b.atropelamentos >= l.p75_atropelamentos                                               AS quartil_superior_atropelamentos,
  b.reclamacoes >= l.p75_reclamacoes                                                     AS quartil_superior_reclamacoes,
  (b.atropelamentos >= l.p75_atropelamentos AND b.reclamacoes >= l.p75_reclamacoes)      AS quartil_superior_nas_duas,
  l.p75_atropelamentos,
  l.p75_reclamacoes
FROM base b
CROSS JOIN limites l
ORDER BY quartil_superior_nas_duas DESC, b.atropelamentos DESC, b.reclamacoes DESC, b.subprefeitura

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q4b — Onde atropelamentos de pedestres e reclamações de calçada se encontram? (calçada + guia e sarjeta)
-- MAGIC
-- MAGIC Mesma consulta da Q4a, mas `reclamacoes` soma também as solicitações de **guia e sarjeta**. Compare quais subprefeituras entram ou saem da marcação quando guia e sarjeta contam.
-- MAGIC
-- MAGIC **Interpretação.**
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

WITH subprefeituras AS (
  SELECT DISTINCT subprefeitura
  FROM workspace.gs.dim_local
  WHERE subprefeitura <> 'NAO_IDENTIFICADO'
),
atropelamentos AS (
  SELECT l.subprefeitura, COUNT(*) AS atropelamentos
  FROM workspace.gs.fato_sinistro_pedestre s
  JOIN workspace.gs.dim_local l ON l.local_id = s.local_id
  WHERE s.data BETWEEN DATE'2026-06-01' AND DATE'2026-09-30'
  GROUP BY l.subprefeitura
),
reclamacoes AS (
  SELECT l.subprefeitura, COUNT(*) AS reclamacoes
  FROM workspace.gs.fato_ocorrencia f
  JOIN workspace.gs.dim_local l      ON l.local_id = f.local_id
  JOIN workspace.gs.dim_categoria c  ON c.categoria_id = f.categoria_id
  WHERE f.origem = 'sp156'
    AND f.data BETWEEN DATE'2026-06-01' AND DATE'2026-09-30'
    AND c.grupo IN ('calcada', 'guia_sarjeta')
  GROUP BY l.subprefeitura
),
base AS (
  SELECT
    s.subprefeitura,
    COALESCE(a.atropelamentos, 0) AS atropelamentos,
    COALESCE(r.reclamacoes, 0)    AS reclamacoes
  FROM subprefeituras s
  LEFT JOIN atropelamentos a ON a.subprefeitura = s.subprefeitura
  LEFT JOIN reclamacoes r    ON r.subprefeitura = s.subprefeitura
),
limites AS (
  SELECT
    percentile_cont(0.75) WITHIN GROUP (ORDER BY atropelamentos) AS p75_atropelamentos,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY reclamacoes)    AS p75_reclamacoes
  FROM base
)
SELECT
  b.subprefeitura,
  b.atropelamentos,
  b.reclamacoes,
  b.atropelamentos >= l.p75_atropelamentos                                               AS quartil_superior_atropelamentos,
  b.reclamacoes >= l.p75_reclamacoes                                                     AS quartil_superior_reclamacoes,
  (b.atropelamentos >= l.p75_atropelamentos AND b.reclamacoes >= l.p75_reclamacoes)      AS quartil_superior_nas_duas,
  l.p75_atropelamentos,
  l.p75_reclamacoes
FROM base b
CROSS JOIN limites l
ORDER BY quartil_superior_nas_duas DESC, b.atropelamentos DESC, b.reclamacoes DESC, b.subprefeitura

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q5 — Chove forte, chegam mais solicitações?
-- MAGIC
-- MAGIC Média diária de solicitações do SP156 em dias de **chuva forte** (`chuva_forte`: pelo menos 10 mm no dia, ver `03_ouro`) contra os demais dias, só no período em que o SP156 e o clima têm dados. Dias sem nenhuma solicitação contam como zero. O clima é de um ponto só, na Praça da Sé, e a amostra de dias de chuva forte é pequena; leia como indício.
-- MAGIC
-- MAGIC **Interpretação.**
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

WITH cobertura AS (
  SELECT MIN(data) AS inicio, MAX(data) AS fim
  FROM workspace.gs.fato_ocorrencia
  WHERE origem = 'sp156'
),
por_dia AS (
  SELECT
    t.data,
    t.chuva_forte,
    COUNT(f.id_ocorrencia_hash) AS solicitacoes
  FROM workspace.gs.dim_tempo t
  CROSS JOIN cobertura c
  LEFT JOIN workspace.gs.fato_ocorrencia f ON f.data = t.data AND f.origem = 'sp156'
  WHERE t.data BETWEEN c.inicio AND c.fim
    AND t.precipitacao_dia_mm IS NOT NULL
  GROUP BY t.data, t.chuva_forte
)
SELECT
  CASE WHEN chuva_forte THEN 'chuva_forte' ELSE 'demais_dias' END AS grupo,
  COUNT(*)                         AS dias,
  SUM(solicitacoes)                AS solicitacoes,
  ROUND(AVG(solicitacoes), 1)      AS media_diaria,
  MIN(c.inicio)                    AS periodo_inicio,
  MIN(c.fim)                       AS periodo_fim
FROM por_dia
CROSS JOIN cobertura c
GROUP BY CASE WHEN chuva_forte THEN 'chuva_forte' ELSE 'demais_dias' END
ORDER BY grupo
