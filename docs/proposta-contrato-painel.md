# Proposta: ampliar `PrioridadeSubprefeitura` (`GET /painel`)

Para aplicar no `openapi.yaml` do repositório principal ([passo-seguro](https://github.com/Passo-Seguro/passo-seguro)), por pull request, como o contrato exige.

## Por quê

A view `workspace.gs.vw_prioridades` (camada Ouro, repositório de dados) calcula, por subprefeitura, indicadores a partir de dados **reais** (SP156 e Infosiga). O contrato atual de `GET /painel` só tem campos de **relatos**, que hoje são simulados e existem só nas áreas-piloto. Sem os campos abaixo, o painel não consegue mostrar o que os dados reais dizem.

Hoje o `painel.json` exportado leva **só as colunas do contrato atual**, para a API não responder `503 painel_invalido`. Os cinco campos abaixo ficam na view e esperam este contrato.

## O que muda

Cinco propriedades **opcionais** em `components.schemas.PrioridadeSubprefeitura.properties`. Nenhuma entra em `required`, então quem já consome o `GET /painel` não é afetado.

```yaml
        reclamacoes_calcada_sp156:
          type: integer
          minimum: 0
          description: |
            Solicitações de **calçada** registradas no SP156 (Prefeitura) na subprefeitura, em todo o
            período carregado pelo pipeline. Reúne os serviços de calçada pública, calçada particular e
            acessibilidade. Guia e sarjeta ficam em `reclamacoes_guia_sarjeta_sp156`.
          example: 282
        reclamacoes_guia_sarjeta_sp156:
          type: integer
          minimum: 0
          description: |
            Solicitações de **guia e sarjeta** registradas no SP156 na subprefeitura, no mesmo período.
          example: 122
        atropelamentos_pedestres:
          type: integer
          minimum: 0
          description: |
            Sinistros de trânsito com ao menos um pedestre (Infosiga) cujo ponto cai na subprefeitura.
            Sinistros sem coordenada não entram em nenhuma subprefeitura.
          example: 89
        pedestres_mortos:
          type: integer
          minimum: 0
          description: Pedestres mortos nesses sinistros.
          example: 2
        indice_prioridade:
          type: number
          minimum: 0
          maximum: 1
          description: |
            Índice relativo entre as 32 subprefeituras: média de `atropelamentos_pedestres` e
            `reclamacoes_calcada_sp156`, cada um normalizado de 0 a 1 pelo mínimo e máximo entre as
            subprefeituras. 1 lidera os dois indicadores; 0 está no piso dos dois. `prioridade` é a faixa
            deste índice (terço de cima `alta`, do meio `media`, de baixo `baixa`).
          example: 0.972
```

## Exemplo de item (Sé, dados do lote 1, até 31/08/2026)

```yaml
- subprefeitura: SE
  prioridade: alta
  relatos_abertos: 585
  relatos_por_categoria: {buraco: 134, piso_danificado: 153, obstaculo: 149, veiculo_na_calcada: 149}
  tempo_medio_ate_reparo_dias: 16.4
  atualizado_em: '2026-11-09T22:30:00-03:00'
  reclamacoes_calcada_sp156: 282
  reclamacoes_guia_sarjeta_sp156: 122
  atropelamentos_pedestres: 89
  pedestres_mortos: 2
  indice_prioridade: 0.972
```

`relatos_abertos` e `relatos_por_categoria` vêm de relatos **simulados** enquanto a API não estiver no ar.

## Outros ajustes sugeridos no mesmo PR

- Na `description` de `prioridade`, dizer que a faixa é **relativa** (posição do `indice_prioridade` entre as 32 subprefeituras), e não um limite absoluto.
- Na `description` de `tempo_medio_ate_reparo_dias`, dizer que vem das solicitações **SP156 finalizadas** (relatos não têm data de conclusão).
- Em `GET /painel`, avisar que os itens vêm ordenados por `indice_prioridade` decrescente.

## Ordem para ativar

1. Aprovar e mesclar este PR no `openapi.yaml` do repositório principal.
2. Conferir que a validação do `painel.json` na API aceita os cinco campos. O schema atual não fecha `additionalProperties`, mas a implementação pode ser mais estrita; por isso o pipeline não os envia antes.
3. No `databricks/05_export.py`, mudar **uma linha**: `INCLUIR_EXTRAS = True`, e rodar o notebook de novo.
