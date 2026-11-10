#!/usr/bin/env bash
# Publica o painel.json da Ouro para a API: baixa do Volume do Databricks, valida contra o openapi.yaml,
# envia ao Azure Blob Storage e confere GET /api/painel. Rode DEPOIS do notebook 05_export.
#
# Uso:
#   export PAINEL_STORAGE_ACCOUNT=<saída storage_account_name do Terraform>
#   export PAINEL_APP_URL=<saída app_url do Terraform, ex.: https://....azurecontainerapps.io>
#   scripts/publicar_painel.sh
#
# Sem chave nem SAS: o envio usa o seu `az login` (papel Storage Blob Data Contributor no container painel).
# Opcional: DATABRICKS_PROFILE (padrão gs).
# Requer: databricks CLI, az, curl e python com `pip install pyyaml jsonschema`.
set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PERFIL="${DATABRICKS_PROFILE:-gs}"
ORIGEM="dbfs:/Volumes/workspace/gs/export/painel.json"
REPO_PRINCIPAL="Passo-Seguro/passo-seguro"
CONTAINER="painel"
BLOB="painel.json"
PYTHON="${PYTHON:-$(command -v python3 || command -v python || true)}"

: "${PAINEL_STORAGE_ACCOUNT:?defina PAINEL_STORAGE_ACCOUNT (nome da conta de armazenamento)}"
: "${PAINEL_APP_URL:?defina PAINEL_APP_URL (URL do app; o GET final é PAINEL_APP_URL/api/painel)}"
[ -n "$PYTHON" ] || { echo "python não encontrado no PATH" >&2; exit 1; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
# No Windows (Git Bash) as CLIs nativas precisam do caminho no formato C:/...
nativo() { if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else echo "$1"; fi; }

echo "1/5 Atualizando openapi.yaml a partir de $REPO_PRINCIPAL (main)"
curl -fsS -H "Accept: application/vnd.github.raw" "https://api.github.com/repos/$REPO_PRINCIPAL/contents/openapi.yaml?ref=main" -o "$TMP/openapi.yaml"
head -c 8 "$TMP/openapi.yaml" | grep -q '^openapi:' || { echo "o que veio do GitHub não parece um openapi.yaml" >&2; exit 1; }
cp "$TMP/openapi.yaml" "$RAIZ/openapi.yaml"

echo "2/5 Baixando $ORIGEM (profile $PERFIL)"
MSYS_NO_PATHCONV=1 databricks fs cp "$ORIGEM" "$(nativo "$TMP/painel.json")" --overwrite --profile "$PERFIL"

echo "3/5 Validando contra openapi.yaml"
"$PYTHON" "$RAIZ/scripts/validar_painel.py" validar "$TMP/painel.json" "$RAIZ/openapi.yaml"

echo "4/5 Enviando para $PAINEL_STORAGE_ACCOUNT/$CONTAINER/$BLOB (az login, sobrescrevendo)"
az storage blob upload --auth-mode login --account-name "$PAINEL_STORAGE_ACCOUNT" \
  -c "$CONTAINER" -n "$BLOB" -f "$(nativo "$TMP/painel.json")" --overwrite --only-show-errors -o none

echo "5/5 Conferindo GET ${PAINEL_APP_URL%/}/api/painel"
status="$(curl -sS -o "$TMP/resposta.json" -w '%{http_code}' "${PAINEL_APP_URL%/}/api/painel")"
if [ "$status" != "200" ]; then
  echo "GET /api/painel respondeu HTTP $status (o arquivo já foi enviado). 404 = blob ainda não visto; 503 = fora do schema; 403 do blob = papel ainda propagando." >&2
  exit 1
fi
"$PYTHON" "$RAIZ/scripts/validar_painel.py" conferir "$TMP/resposta.json" "$TMP/painel.json"
