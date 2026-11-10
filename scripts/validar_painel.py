"""Apoio de publicar_painel.ps1 / publicar_painel.sh: valida o painel.json e confere a resposta da API.

Uso:
  python scripts/validar_painel.py validar  <painel.json> <openapi.yaml>
  python scripts/validar_painel.py conferir <resposta.json> <painel_enviado.json>

`validar` confere cada objeto contra `components.schemas.PrioridadeSubprefeitura` do openapi.yaml (o mesmo
schema que a API usa para responder 503 painel_invalido), campos fora do contrato e a ordem por
`indice_prioridade` decrescente. Sai com 1 se algo estiver fora; nada é enviado nesse caso.

`conferir` resume o que o GET /api/painel devolveu: quantos itens vieram, se os campos extras estão presentes
e se a resposta é a do arquivo recém-enviado (a API guarda o painel em cache por até 10 minutos).

Requer: pip install pyyaml jsonschema
"""
import json
import re
import sys
from pathlib import Path

try:
    import yaml
    from jsonschema import Draft202012Validator
except ImportError:
    sys.exit("Faltam dependências. Rode: pip install pyyaml jsonschema")

SCHEMA = "PrioridadeSubprefeitura"
EXTRAS = [
    "reclamacoes_calcada_sp156",
    "reclamacoes_guia_sarjeta_sp156",
    "atropelamentos_pedestres",
    "pedestres_mortos",
    "indice_prioridade",
]
ISO_COM_FUSO = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")


def carregar_lista(caminho):
    try:
        dados = json.loads(Path(caminho).read_text(encoding="utf-8"))
    except (OSError, ValueError) as erro:
        sys.exit(f"{caminho}: não consegui ler como JSON UTF-8 ({type(erro).__name__})")
    if not isinstance(dados, list):
        sys.exit(f"{caminho}: esperava uma lista de objetos, veio {type(dados).__name__}")
    return dados


def erros_do_painel(itens, openapi):
    """Lista de problemas (texto), vazia se o painel estiver dentro do contrato."""
    schemas = openapi["components"]["schemas"]
    if SCHEMA not in schemas:
        sys.exit(f"openapi.yaml sem components.schemas.{SCHEMA}")
    # O documento inteiro vai junto para os $ref ('#/components/schemas/...') se resolverem.
    validador = Draft202012Validator({"$ref": f"#/components/schemas/{SCHEMA}", "components": openapi["components"]})
    permitidos = set(schemas[SCHEMA]["properties"])
    erros = []
    for i, item in enumerate(itens):
        nome = item.get("subprefeitura", f"#{i}") if isinstance(item, dict) else f"#{i}"
        for erro in sorted(validador.iter_errors(item), key=lambda e: list(e.path)):
            onde = ".".join(str(p) for p in erro.path) or "(objeto)"
            erros.append(f"{nome}: {onde}: {erro.message}")
        if isinstance(item, dict):
            if sobra := sorted(set(item) - permitidos):
                erros.append(f"{nome}: campos fora do contrato: {sobra}")
            hora = item.get("atualizado_em")
            if isinstance(hora, str) and not ISO_COM_FUSO.match(hora):
                erros.append(f"{nome}: atualizado_em sem fuso ou fora do ISO 8601: {hora!r}")
    indices = [i.get("indice_prioridade") for i in itens if isinstance(i, dict)]
    if all(isinstance(x, (int, float)) for x in indices) and indices != sorted(indices, reverse=True):
        erros.append("itens fora da ordem de indice_prioridade decrescente")
    return erros


def resumo_extras(itens):
    """(itens com os 5 extras, campos extras que faltam em algum item)."""
    completos = sum(all(c in i for c in EXTRAS) for i in itens)
    faltam = [c for c in EXTRAS if not all(c in i for i in itens)]
    return completos, faltam


def validar(painel, openapi_yaml):
    itens = carregar_lista(painel)
    if not itens:
        sys.exit(f"{painel}: lista vazia, nada a publicar")
    openapi = yaml.safe_load(Path(openapi_yaml).read_text(encoding="utf-8"))
    erros = erros_do_painel(itens, openapi)
    if erros:
        print(f"{len(erros)} problema(s) contra {SCHEMA} de {openapi_yaml}:", file=sys.stderr)
        for e in erros[:20]:
            print("  -", e, file=sys.stderr)
        if len(erros) > 20:
            print(f"  ... e mais {len(erros) - 20}", file=sys.stderr)
        sys.exit(1)
    completos, faltam = resumo_extras(itens)
    print(f"{len(itens)} itens válidos contra {SCHEMA}; {completos}/{len(itens)} com os 5 campos extras", end="")
    print(f" (faltam: {', '.join(faltam)})" if faltam else "")


def conferir(resposta, enviado):
    itens = carregar_lista(resposta)
    completos, faltam = resumo_extras(itens)
    print(f"GET /api/painel: {len(itens)} itens")
    if itens and not faltam:
        print(f"campos extras: presentes nos {len(itens)} itens ({', '.join(EXTRAS)})")
    else:
        print(f"campos extras: {completos}/{len(itens)} itens completos; ausentes em algum item: {', '.join(faltam) or '-'}")
    novo = carregar_lista(enviado)
    mesmo = len(itens) == len(novo) and [i.get("atualizado_em") for i in itens] == [i.get("atualizado_em") for i in novo]
    if mesmo:
        print("a resposta é a do arquivo que acabou de ser enviado")
    else:
        print("AVISO: a resposta ainda não é a do arquivo enviado. A API guarda o painel em cache por até 10 minutos;")
        print("       espere e rode de novo (só o GET) antes de achar que o envio falhou.")


if __name__ == "__main__":
    if len(sys.argv) != 4 or sys.argv[1] not in ("validar", "conferir"):
        sys.exit(__doc__)
    {"validar": validar, "conferir": conferir}[sys.argv[1]](sys.argv[2], sys.argv[3])
