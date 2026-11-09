# Passo Seguro — Big Data & Data Integration

Ficha Técnica: [README do repositório principal](https://github.com/Passo-Seguro/passo-seguro#ficha-técnica)

## Integrantes
| Nome completo | RM |
|---|---|
| Guilherme Morais | 551981 |
| Júlia Marques | 98680 |
| Matheus Gusmão | 550826 |

## Repositórios do projeto
- Principal: [passo-seguro](https://github.com/Passo-Seguro/passo-seguro)
- GenAI: [passo-seguro-genai](https://github.com/Passo-Seguro/passo-seguro-genai) · Dados: [passo-seguro-dados](https://github.com/Passo-Seguro/passo-seguro-dados) · Visão: [passo-seguro-visao](https://github.com/Passo-Seguro/passo-seguro-visao) · RPA: [passo-seguro-rpa](https://github.com/Passo-Seguro/passo-seguro-rpa) · Robô: [passo-seguro-robo](https://github.com/Passo-Seguro/passo-seguro-robo)

## Pipeline de coleta (Airflow)

A DAG `gs_coleta` (`@daily`, `start_date` 2026-06-01, `catchup=True`) grava **um arquivo por fonte e por dia** em `chegada/<fonte>/`. O upload ao Databricks é manual, pela interface.

| Fonte | Arquivo | Origem |
|---|---|---|
| `sp156` | `sp156_AAAA-MM-DD.csv` | Arquivo trimestral da Prefeitura (CKAN), só os 4 serviços de calçada (abaixo) |
| `infosiga` | `infosiga_AAAA-MM-DD.csv` | CSVs ou ZIP em `entrada_manual/infosiga/` (download manual); um sinistro por linha, só os de SAO PAULO com ao menos um pedestre (abaixo) |
| `open_meteo` | `open_meteo_AAAA-MM-DD.json` | Chuva horária num ponto na Praça da Sé |
| `relatos` | `relatos_AAAA-MM-DD.json` | `GET /relatos` da API; **simulado** enquanto a API não estiver no ar |

### Como ler os dados

- **SP156 e Infosiga são carregamentos retroativos, particionados pela data do registro.** O dia no nome do arquivo (`sp156_2026-07-15.csv`) é a data em que a solicitação foi **aberta** (SP156) ou em que o sinistro **aconteceu** (Infosiga), e não o dia em que a DAG rodou. As duas fontes publicam arquivos fechados, semanas depois; por isso o histórico desde 2026-06-01 é carregado de uma vez (catchup) e cada dia sai da carga mais recente disponível. Dia que a fonte ainda não publicou fica `skipped`, sem arquivo. Reexecutar um dia reescreve o mesmo arquivo. No Databricks, particione pelo dia do nome do arquivo.
- **Infosiga: uma linha por sinistro com pedestre.** Entram os sinistros do município `SAO PAULO` com ao menos uma vítima `tipo_de_vitima = 'PEDESTRE'` (arquivo `pessoas`, ligado por `pessoas.id_sinistro` → `sinistros.id_sinistro`). Colunas, separadas por `;`, em UTF-8 como o SP156: `id_sinistro`, `data_sinistro`, `hora`, `municipio`, `logradouro`, `latitude`, `longitude`, `qtd_pedestres`, `qtd_pedestres_feridos` (gravidade `GRAVE` ou `LEVE`), `qtd_pedestres_mortos` (`FATAL`). Pedestres `ILESO` ou `NAO DISPONIVEL` contam só em `qtd_pedestres`. Não há veículos nem dado individual de pessoa (idade, sexo etc.). `latitude` e `longitude` ficam **como vêm, com vírgula decimal** (`-23,5019`), e sinistros sem coordenada (cerca de 8%) **são mantidos** com os dois campos vazios: converter e tratar na Prata. Dia em que a fonte tem sinistros mas nenhum com pedestre gera arquivo só com o cabeçalho.
- **SP156 filtra por Serviço, não por Assunto.** Em 05/2026 a Prefeitura passou os mesmos serviços de calçada de "Calçadas, guias e postes" para "Fiação e Postes", então o Assunto não serve de filtro. A constante `SERVICOS_CALCADA` (`dags/gs_coleta_lib/sp156.py`) tem exatamente: *Calçada pública - solicitar manutenção*; *Calçada particular - denunciar calçada danificada ou inexistente*; *Acessibilidade - solicitar avaliação de obstáculo na calçada*; *Guias, sarjetas e sarjetões - solicitar manutenção*. Tapa-buraco (asfalto) e poda de árvore ficam de fora de propósito. O trimestre é lido do XLSX (datas tipadas) e convertido ao mesmo CSV do 2º TRI (`;`, `DD/MM/AAAA`); o arquivo fica em cache no volume `airflow-cache` e não é baixado de novo (para reprocessar um trimestre, apague-o de lá).
- **Relatos são simulados até a API estar no ar.** Com `PASSO_SEGURO_API_URL` vazia, a DAG gera relatos fictícios e determinísticos nas áreas-piloto (Sé e Pinheiros), todos com `"simulado": true` e `"origem": "mock"` no arquivo. Não use para análise real. Quando a API subir, preencha `PASSO_SEGURO_API_URL` e `PASSO_SEGURO_API_TOKEN` e limpe `coletar_relatos` (`airflow tasks clear gs_coleta -t '^coletar_relatos$' -y`) para recoletar o histórico com `"origem": "api"`.
- **O clima usa um único ponto, na Praça da Sé** (lat -23,5505, lon -46,6333), para representar a cidade inteira; a chuva varia entre zonas. Vem do arquivo histórico da Open-Meteo, e dos dias mais recentes (ainda sem reanálise) vem da previsão; o campo `coleta.api` do JSON diz qual API respondeu.

### Subir o Airflow

```bash
cp .env.example .env              # e preencha AIRFLOW_FERNET_KEY
docker compose build              # imagem do Airflow com openpyxl (Dockerfile)
docker compose up airflow-init    # uma vez
docker compose up -d              # http://localhost:8080 (admin/admin, só em 127.0.0.1)

docker compose exec airflow-scheduler airflow variables set ALERTA_WEBHOOK_URL '<webhook do Discord>'
docker compose exec airflow-scheduler airflow variables set PASSO_SEGURO_API_URL ''
docker compose exec airflow-scheduler airflow dags unpause gs_coleta   # catchup desde 2026-06-01
```

Falha de qualquer tarefa (após as tentativas) posta um alerta no webhook. Para testar: `airflow variables set FORCAR_FALHA_FONTE relatos`, dispare a DAG e depois `airflow variables delete FORCAR_FALHA_FONTE`.

## Databricks

Camada Bronze do Medallion no Databricks Free Edition (catálogo `workspace`). A CLI usa o profile `gs` (`databricks ... --profile gs`).

### Schema e Volumes

```bash
databricks schemas create gs workspace --profile gs
databricks volumes create workspace gs chegada MANAGED --profile gs   # arquivos coletados
databricks volumes create workspace gs privado MANAGED --profile gs   # segredos (sal)
for p in sp156 infosiga open_meteo relatos geo; do
  databricks fs mkdir dbfs:/Volumes/workspace/gs/chegada/$p --profile gs
done
```

Se já existirem, nada muda. O Auto Loader cria sozinho `chegada/_controle/` (schemas e checkpoints); não mexa nessa pasta.

### Sal

Fica em `/Volumes/workspace/gs/privado/sal.txt` (64 caracteres hex, gerado com `secrets.token_hex(32)`). **O valor não está no repositório nem em qualquer cópia local**; a Prata lê o arquivo direto do Volume. Não o regenere depois da Prata: trocar o sal muda todos os hashes derivados dele.

### Ordem de upload dos lotes

Os arquivos de `chegada/` **não** vão para o Git e sobem manualmente, pela interface (Catalog → `workspace.gs.chegada` → pasta da fonte → *Upload to this volume*), mantendo a pasta de cada fonte e o nome do arquivo.

1. Suba o **lote 1** (os arquivos mais antigos, nas pastas de cada fonte) e rode a Bronze (abaixo). Tire print da tabela de contagem do fim do notebook.
2. Suba o **lote 2** (o restante, em ordem de data) e rode a Bronze de novo. Tire print: só as linhas do lote 2 entram.
3. Rode a Bronze **mais uma vez, sem subir nada**. Tire print: os números não mudam (o checkpoint do Auto Loader já tem esses arquivos).

### Rodar a Bronze

O notebook fonte está em [`databricks/01_bronze.py`](databricks/01_bronze.py) e fica no workspace em `/Users/<seu e-mail>/passo-seguro/01_bronze`. Para atualizar a cópia do workspace depois de editar o arquivo:

```bash
databricks workspace import "/Users/<seu e-mail>/passo-seguro/01_bronze" \
  --file databricks/01_bronze.py --format SOURCE --language PYTHON --overwrite --profile gs
```

No Git Bash (Windows), prefixe com `MSYS_NO_PATHCONV=1` para o `/Users/...` não ser reescrito.

Abra o notebook, conecte ao compute serverless e use **Run all**. Ele lê cada pasta com Auto Loader e grava `workspace.gs.bronze_sp156`, `bronze_infosiga`, `bronze_open_meteo` e `bronze_relatos`, sempre em texto e com as colunas `arquivo_origem` e `carregado_em`. Notas:

- Reexecutar não duplica: o checkpoint de cada fonte (`chegada/_controle/checkpoint_<fonte>`) guarda os arquivos já lidos.
- Os nomes das colunas dos CSVs são normalizados (`Data de Finalização` → `data_de_finalizacao`); os valores não.
- Arquivo só com cabeçalho (ex.: `infosiga_2026-09-10.csv`) é lido sem erro, mas não gera linha, então não conta em `arquivos distintos`.
- Em `open_meteo` e `relatos` cada arquivo é uma linha; `hourly` e `itens` seguem aninhados até a Prata.
- Para recomeçar uma fonte do zero: apague a tabela e as pastas `schema_<fonte>` e `checkpoint_<fonte>` em `_controle/`.
