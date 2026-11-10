# Publica o painel.json da Ouro para a API: baixa do Volume do Databricks, valida contra o openapi.yaml,
# envia ao Azure Blob Storage e confere GET /api/painel. Rode DEPOIS do notebook 05_export.
#
# Uso:
#   $env:PAINEL_STORAGE_ACCOUNT = '<saída storage_account_name do Terraform>'
#   $env:PAINEL_APP_URL = '<saída app_url do Terraform, ex.: https://....azurecontainerapps.io>'
#   .\scripts\publicar_painel.ps1
#
# Sem chave nem SAS: o envio usa o seu `az login` (papel Storage Blob Data Contributor no container painel).
# Opcional: $env:DATABRICKS_PROFILE (padrão gs).
# Requer: databricks CLI, az, python com `pip install pyyaml jsonschema`.
$ErrorActionPreference = 'Stop'
$env:PYTHONUTF8 = '1'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)

$Raiz = Split-Path -Parent $PSScriptRoot
$Perfil = if ($env:DATABRICKS_PROFILE) { $env:DATABRICKS_PROFILE } else { 'gs' }
$Origem = 'dbfs:/Volumes/workspace/gs/export/painel.json'
$RepoPrincipal = 'Passo-Seguro/passo-seguro'
$Container = 'painel'
$Blob = 'painel.json'

if (-not $env:PAINEL_STORAGE_ACCOUNT) { throw 'defina PAINEL_STORAGE_ACCOUNT (nome da conta de armazenamento)' }
if (-not $env:PAINEL_APP_URL) { throw 'defina PAINEL_APP_URL (URL do app; o GET final é PAINEL_APP_URL/api/painel)' }
$Conta = $env:PAINEL_STORAGE_ACCOUNT
$UrlApi = $env:PAINEL_APP_URL.TrimEnd('/') + '/api/painel'

# As CLIs nativas não derrubam o script sozinhas: confere o código de saída de cada uma.
function Confirmar([string]$Passo) { if ($LASTEXITCODE -ne 0) { throw "$Passo falhou (código $LASTEXITCODE)" } }

$Tmp = Join-Path ([IO.Path]::GetTempPath()) ("painel-" + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $Tmp | Out-Null
try {
    $Utf8 = New-Object System.Text.UTF8Encoding($false)

    Write-Host "1/5 Atualizando openapi.yaml a partir de $RepoPrincipal (main)"
    # Bytes direto para o arquivo: sem passar pela codificação do console, que estragaria os acentos.
    $r = Invoke-WebRequest -Uri "https://api.github.com/repos/$RepoPrincipal/contents/openapi.yaml?ref=main" -Headers @{ Accept = 'application/vnd.github.raw' } -UseBasicParsing
    $bytes = $r.RawContentStream.ToArray()
    if ($Utf8.GetString($bytes, 0, [Math]::Min(8, $bytes.Length)) -ne 'openapi:') { throw 'o que veio do GitHub não parece um openapi.yaml' }
    [IO.File]::WriteAllBytes((Join-Path $Raiz 'openapi.yaml'), $bytes)

    Write-Host "2/5 Baixando $Origem (profile $Perfil)"
    $Painel = Join-Path $Tmp 'painel.json'
    & databricks fs cp $Origem $Painel --overwrite --profile $Perfil
    Confirmar 'databricks fs cp'

    Write-Host '3/5 Validando contra openapi.yaml'
    & python (Join-Path $PSScriptRoot 'validar_painel.py') validar $Painel (Join-Path $Raiz 'openapi.yaml')
    Confirmar 'validação'

    Write-Host "4/5 Enviando para $Conta/$Container/$Blob (az login, sobrescrevendo)"
    & az storage blob upload --auth-mode login --account-name $Conta -c $Container -n $Blob -f $Painel --overwrite --only-show-errors -o none
    Confirmar 'az storage blob upload'

    Write-Host "5/5 Conferindo GET $UrlApi"
    $Resposta = Join-Path $Tmp 'resposta.json'
    try {
        $r = Invoke-WebRequest -Uri $UrlApi -UseBasicParsing
        [IO.File]::WriteAllBytes($Resposta, $r.RawContentStream.ToArray())
    } catch {
        $codigo = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { 'sem resposta' }
        throw "GET /api/painel respondeu HTTP $codigo (o arquivo já foi enviado). 404 = blob ainda não visto; 503 = fora do schema; 403 do blob = papel ainda propagando."
    }
    & python (Join-Path $PSScriptRoot 'validar_painel.py') conferir $Resposta $Painel
    Confirmar 'conferência'
}
finally {
    Remove-Item -Recurse -Force $Tmp -ErrorAction SilentlyContinue
}
