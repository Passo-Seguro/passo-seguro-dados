# Amostras

Cerca de 50 linhas de cada fonte, tiradas dos arquivos que a DAG `gs_coleta` grava em `chegada/` (formato idêntico), para testar a Bronze/Prata sem rodar o Airflow. Nenhuma tem dado pessoal: foram varridas por telefone, CPF, CNPJ, e-mail e URL.

| Arquivo | Conteúdo |
|---|---|
| `sp156_amostra.csv` | 50 solicitações dos 4 serviços de calçada, sorteadas de 01/06 a 30/09/2026. `Número` e `CEP` ficam **vazios** (endereço exato de quem pediu); `Logradouro` e coordenadas permanecem. |
| `infosiga_amostra.csv` | 50 sinistros com pedestre em SAO PAULO, 46 com coordenada e 4 **sem** (a Prata precisa tratar os dois casos). Latitude e longitude com vírgula decimal, como vêm. |
| `open_meteo_2026-07-15.json`, `open_meteo_2026-07-16.json` | Dois dias completos (48 horas) de chuva, na Praça da Sé. |
| `relatos_mock.json` | 51 relatos **simulados** (`"simulado": true`), no formato `Relato` do contrato da API: 40 válidos e 11 de propósito com problema, para as regras de qualidade. |

Gerados com semente fixa a partir dos arquivos de `chegada/`; os dados do SP156 e do Infosiga são reais e públicos, os relatos são fictícios.

## Relatos inválidos de propósito

Cada um deve ser pego por uma regra de qualidade (validadas ao gerar: só estes falham, os outros 40 passam). Área de atendimento: município de São Paulo.

| id | Problema |
|---|---|
| `fc299775-a008-4219-9bad-27d1fb1f3714` | lat/lon no Rio de Janeiro (fora da área de atendimento) |
| `e6d58617-a852-4ef1-9161-26b75872c996` | lat = 0 e lon = 0 |
| `60ba7e6b-8786-4ec9-92c5-a30fab4240c5` | lat e lon trocadas |
| `49c365f4-8be2-4d01-84d5-3bea7f6e6344` | lat = 123,4 (fora de -90..90) |
| `e542a274-4204-4e84-8866-9272faff71d3` | confianca = 1,3 |
| `85e5da4b-0f54-4394-9893-213a2782b6ac` | categoria = 'calcada_ruim' |
| `b9e09793-9adb-49c0-a605-a87f59ada41e` | status = 'arquivado' |
| `de1c2c1d-878a-4847-b7c6-7906b5730fbd` | criado_em sem fuso horário |
| `cb03d22b-af18-4165-84c9-598ec515d8ba` | confirmacoes = -2 |
| `0c01b7f7-2e75-4ac7-b435-1f530d174dcb` | status 'novo' com prioridade e subprefeitura preenchidas |
| `dfdc34e7-19ad-4800-b973-61a77abc41ec` | mesmo id de outro relato do arquivo (aparece duas vezes) |
