"""Calcula, só a partir de chegada/ e dos GeoJSON locais (sem usar os notebooks), o que a Ouro deve mostrar.

Serve para conferir a execução do Databricks: refaz a Prata (tipagem, regras de qualidade, deduplicação),
atribui subprefeitura/distrito por ponto no polígono, monta as tabelas da Ouro em memória e responde as
consultas de 04_consultas.sql.

Uso:  python scripts/conferir_ouro.py [--corte AAAA-MM-DD]      (padrão: 2026-08-31, o lote 1)
Requer: shapely. Não lê o sal nem grava nada.
"""
import argparse
import csv
import glob
import json
import re
import statistics
import unicodedata
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import shapely
from shapely.geometry import shape

RAIZ = Path(__file__).resolve().parent.parent
CHEGADA = RAIZ / "chegada"
GEO = RAIZ / "entrada_manual" / "geo"

# --- Constantes que espelham os notebooks (manter iguais) -------------------------------------------------
LAT_MIN, LAT_MAX = -24.01, -23.35
LON_MIN, LON_MAX = -46.83, -46.36
SERVICOS_CALCADA = (
    "Calçada pública - solicitar manutenção",
    "Calçada particular - denunciar calçada danificada ou inexistente",
    "Acessibilidade - solicitar avaliação de obstáculo na calçada",
    "Guias, sarjetas e sarjetões - solicitar manutenção",
)
SERVICO_GUIA_SARJETA = "Guias, sarjetas e sarjetões - solicitar manutenção"
CATEGORIAS_RELATO = ("buraco", "piso_danificado", "obstaculo", "veiculo_na_calcada")
MAPA_STATUS_SP156 = {"finalizada": "finalizada", "cancelada": "cancelada", "aberto": "em_aberto", "em andamento": "em_aberto"}
MAPA_STATUS_RELATO = {  # status do relato (contrato da API) -> status da Ouro
    "novo": "em_aberto", "revisao_humana": "em_aberto", "validado": "em_aberto", "encaminhado": "em_aberto",
    "resolvido": "finalizada", "rejeitado": "indeferida",
}
LIMIAR_CHUVA_FORTE_MM = 10.0
NAO_IDENTIFICADO = "NAO_IDENTIFICADO"
DISTANCIA_MAX_GRAUS = 50 / 111_000  # ponto que cai numa fresta entre polígonos: vale o mais próximo, até ~50 m
PERIODO_Q4 = (date(2026, 6, 1), date(2026, 9, 30))
TZ_SP = timezone(timedelta(hours=-3))  # sem horário de verão desde 2019


# --- Leitura (espelha a Prata) ----------------------------------------------------------------------------
def txt(v):
    v = (v or "").strip()
    return v or None


def numero(v):
    v = txt(v)
    if v is None:
        return None
    try:
        return float(v.replace(",", "."))
    except ValueError:
        return None


def inteiro(v):
    v = txt(v)
    try:
        return int(v) if v is not None else None
    except ValueError:
        return None


def data_br(v):
    try:
        return datetime.strptime(txt(v) or "", "%d/%m/%Y").date()
    except ValueError:
        return None


def fora_do_municipio(lat, lon):
    if lat is None or lon is None:
        return False
    return not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX)


def arquivos(fonte, ext, corte):
    saida = []
    for f in sorted(glob.glob(str(CHEGADA / fonte / f"{fonte}_*.{ext}"))):
        dia = date.fromisoformat(Path(f).stem.split("_")[-1])
        if dia <= corte:
            saida.append(f)
    return saida


def ler_sp156(corte):
    aceitos, barrados = [], Counter()
    for f in arquivos("sp156", "csv", corte):
        with open(f, encoding="utf-8", newline="") as fh:
            for r in csv.DictReader(fh, delimiter=";"):
                lat, lon = numero(r["Latitude"]), numero(r["Longitude"])
                servico = txt(r["Serviço"])
                if fora_do_municipio(lat, lon):
                    barrados["coordenada_fora_do_municipio"] += 1
                    continue
                if servico is None or servico not in SERVICOS_CALCADA:
                    barrados["categoria_ausente_ou_fora_da_lista"] += 1
                    continue
                status = txt(r["Status"])
                aceitos.append({
                    "data_abertura": data_br(r["Data de Abertura"]),
                    "data_finalizacao": data_br(r["Data de Finalização"]),
                    "categoria": "guia_sarjeta" if servico == SERVICO_GUIA_SARJETA else "calcada",
                    "status": MAPA_STATUS_SP156.get(status.lower()) if status else None,
                    "lat": lat, "lon": lon,
                    "subprefeitura_sp156": txt(r["Prefeitura Operacional"]),
                    "distrito_sp156": txt(r["Distrito"]),
                })
    return aceitos, barrados


def ler_infosiga(corte):
    por_id, barrados = {}, Counter()
    for f in arquivos("infosiga", "csv", corte):
        with open(f, encoding="utf-8", newline="") as fh:
            for r in csv.DictReader(fh, delimiter=";"):
                por_id[txt(r["id_sinistro"])] = (r, f)  # o arquivo mais recente vence
    aceitos = []
    for id_sinistro, (r, _) in por_id.items():
        lat, lon = numero(r["latitude"]), numero(r["longitude"])
        if fora_do_municipio(lat, lon):
            barrados["coordenada_fora_do_municipio"] += 1
            continue
        aceitos.append({
            "id_sinistro": id_sinistro, "data": data_br(r["data_sinistro"]), "lat": lat, "lon": lon,
            "qtd_pedestres": inteiro(r["qtd_pedestres"]),
            "qtd_pedestres_feridos": inteiro(r["qtd_pedestres_feridos"]),
            "qtd_pedestres_mortos": inteiro(r["qtd_pedestres_mortos"]),
        })
    return aceitos, barrados


def ler_open_meteo(corte):
    por_hora = {}
    for f in arquivos("open_meteo", "json", corte):
        h = json.load(open(f, encoding="utf-8"))["hourly"]
        for t, p in zip(h["time"], h["precipitation"]):
            por_hora[t] = p
    return por_hora


def ler_relatos(corte):
    por_id, barrados = {}, Counter()
    for f in arquivos("relatos", "json", corte):
        for it in json.load(open(f, encoding="utf-8"))["itens"]:
            por_id[it["id"]] = it  # o arquivo mais recente vence
    aceitos = []
    for id_, it in por_id.items():
        lat, lon = numero(str(it["lat"]) if it["lat"] is not None else None), numero(str(it["lon"]) if it["lon"] is not None else None)
        if fora_do_municipio(lat, lon):
            barrados["coordenada_fora_do_municipio"] += 1
            continue
        if it.get("categoria") not in CATEGORIAS_RELATO:
            barrados["categoria_ausente_ou_fora_da_lista"] += 1
            continue
        criado = datetime.fromisoformat(it["criado_em"]).astimezone(TZ_SP)
        aceitos.append({
            "id": id_, "data": criado.date(), "lat": lat, "lon": lon, "categoria": it["categoria"],
            "status": MAPA_STATUS_RELATO.get(it["status"]), "simulado": bool(it.get("simulado")),
        })
    return aceitos, barrados


# --- Geo --------------------------------------------------------------------------------------------------
def normalizar(s):
    if s is None:
        return None
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().upper()
    return re.sub(r"\s+", " ", s).strip()


class Geo:
    def __init__(self):
        sub = json.load(open(GEO / "subprefeitura.geojson", encoding="utf-8"))["features"]
        dis = json.load(open(GEO / "distrito.geojson", encoding="utf-8"))["features"]
        self.sub_nomes = [normalizar(f["properties"]["nm_subprefeitura"]) for f in sub]
        self.sub_geoms = [shape(f["geometry"]) for f in sub]
        self.dis_nomes = [normalizar(f["properties"]["nm_distrito_municipal"]) for f in dis]
        self.dis_geoms = [shape(f["geometry"]) for f in dis]
        por_cod = {f["properties"]["cd_identificador_subprefeitura"]: normalizar(f["properties"]["nm_subprefeitura"]) for f in sub}
        self.pai_do_distrito = {normalizar(f["properties"]["nm_distrito_municipal"]): por_cod[f["properties"]["cd_identificador_subprefeitura"]] for f in dis}
        self._st, self._dt = shapely.STRtree(self.sub_geoms), shapely.STRtree(self.dis_geoms)
        self.subprefeituras, self.distritos = set(self.sub_nomes), set(self.dis_nomes)

    @staticmethod
    def _achar(tree, nomes, ponto):
        i = tree.query(ponto, predicate="within")
        if len(i):
            return nomes[min(i)], False
        j = tree.query_nearest(ponto, max_distance=DISTANCIA_MAX_GRAUS, all_matches=False)
        return (nomes[j[0]], True) if len(j) else (None, False)

    def local(self, lat, lon):
        """(subprefeitura, distrito, usou_vizinho) pelo ponto; (None, None, False) sem coordenada."""
        if lat is None or lon is None:
            return None, None, False
        p = shapely.Point(lon, lat)
        s, vs = self._achar(self._st, self.sub_nomes, p)
        d, vd = self._achar(self._dt, self.dis_nomes, p)
        return s, d, vs or vd


# --- Ouro -------------------------------------------------------------------------------------------------
def montar_ouro(corte):
    geo = Geo()
    sp156, rej_sp = ler_sp156(corte)
    infosiga, rej_in = ler_infosiga(corte)
    clima = ler_open_meteo(corte)
    relatos, rej_re = ler_relatos(corte)
    diag = {"rejeitados": {"sp156": rej_sp, "infosiga": rej_in, "relatos": rej_re}, "vizinho": 0,
            "sp156_decl_sub_invalida": Counter(), "sp156_decl_dis_invalida": 0, "sp156_sub_diverge": 0,
            "sp156_sub_validas": 0, "sp156_par_incoerente": 0, "sp156_reserva": 0, "sub_x_pai_distrito_diverge": 0, "pontos": 0}

    def atribuir(r, declarado=False):
        s, d, viz = geo.local(r["lat"], r["lon"])
        diag["pontos"] += 1
        diag["vizinho"] += viz
        if declarado:
            # O declarado só serve de comparação e de reserva: o ponto no polígono manda (ver README)
            ds, dd = normalizar(r["subprefeitura_sp156"]), normalizar(r["distrito_sp156"])
            if ds in geo.subprefeituras:
                diag["sp156_sub_validas"] += 1
                diag["sp156_sub_diverge"] += (s is not None and s != ds)
                if dd in geo.distritos:
                    diag["sp156_par_incoerente"] += (geo.pai_do_distrito[dd] != ds)
            else:
                diag["sp156_decl_sub_invalida"][ds] += 1
            if dd not in geo.distritos:
                diag["sp156_decl_dis_invalida"] += 1
            if s is None and ds in geo.subprefeituras:
                s, diag["sp156_reserva"] = ds, diag["sp156_reserva"] + 1
            if d is None and dd in geo.distritos:
                d = dd
        if s and d and geo.pai_do_distrito[d] != s:
            diag["sub_x_pai_distrito_diverge"] += 1
        return s or NAO_IDENTIFICADO, d or NAO_IDENTIFICADO

    ocorrencias = []
    for r in sp156:
        s, d = atribuir(r, declarado=True)
        dias = (r["data_finalizacao"] - r["data_abertura"]).days if r["status"] == "finalizada" and r["data_finalizacao"] else None
        ocorrencias.append({"origem": "sp156", "data": r["data_abertura"], "subprefeitura": s, "distrito": d,
                            "categoria": r["categoria"], "status": r["status"], "dias_ate_conclusao": dias,
                            "simulado": False, "lat": r["lat"], "lon": r["lon"]})
    for r in relatos:
        s, d = atribuir(r)
        ocorrencias.append({"origem": "relato", "data": r["data"], "subprefeitura": s, "distrito": d,
                            "categoria": r["categoria"], "status": r["status"], "dias_ate_conclusao": None,
                            "simulado": r["simulado"], "lat": r["lat"], "lon": r["lon"]})
    sinistros = []
    for r in infosiga:
        s, d = atribuir(r)
        sinistros.append({"id_sinistro": r["id_sinistro"], "data": r["data"], "subprefeitura": s, "distrito": d,
                          "qtd_pedestres": r["qtd_pedestres"], "qtd_pedestres_feridos": r["qtd_pedestres_feridos"],
                          "qtd_pedestres_mortos": r["qtd_pedestres_mortos"]})

    chuva = defaultdict(float)
    for t, p in clima.items():
        chuva[date.fromisoformat(t[:10])] += p or 0.0
    datas = [o["data"] for o in ocorrencias] + [s["data"] for s in sinistros] + list(chuva)
    dim_tempo, d = {}, min(datas)
    while d <= max(datas):
        mm = round(chuva[d], 1) if d in chuva else None
        dim_tempo[d] = {"data": d, "precipitacao_dia_mm": mm, "chuva_forte": None if mm is None else mm >= LIMIAR_CHUVA_FORTE_MM}
        d += timedelta(days=1)
    return {"geo": geo, "fato_ocorrencia": ocorrencias, "fato_sinistro_pedestre": sinistros, "dim_tempo": dim_tempo,
            "diag": diag, "prata": {"sp156": len(sp156), "infosiga": len(infosiga), "open_meteo": len(clima), "relatos": len(relatos)}}


# --- Consultas (as mesmas de 04_consultas.sql) ------------------------------------------------------------
def arred(x, casas=1):
    return None if x is None else float(Decimal(repr(x)).quantize(Decimal(1).scaleb(-casas), rounding=ROUND_HALF_UP))


def percentil(valores, p):
    v = sorted(valores)
    k = (len(v) - 1) * p
    i = int(k)
    return v[i] if i + 1 >= len(v) else v[i] + (v[i + 1] - v[i]) * (k - i)


def subprefeituras(ouro):
    return sorted(ouro["geo"].subprefeituras)


def q1(ouro):
    sp = [o for o in ouro["fato_ocorrencia"] if o["origem"] == "sp156"]
    cal, gui = Counter(), Counter()
    for o in sp:
        (cal if o["categoria"] == "calcada" else gui)[o["subprefeitura"]] += 1
    linhas = [{"subprefeitura": s, "calcada": cal[s], "guia_sarjeta": gui[s], "calcada_mais_guia": cal[s] + gui[s]}
              for s in subprefeituras(ouro)]
    for chave, pos in (("calcada", "posicao_sem_guia"), ("calcada_mais_guia", "posicao_com_guia")):
        for i, l in enumerate(sorted(linhas, key=lambda l: (-l[chave], l["subprefeitura"])), 1):
            l[pos] = i
    # empate no valor: posição de RANK() (igual para empatados)
    for chave, pos in (("calcada", "posicao_sem_guia"), ("calcada_mais_guia", "posicao_com_guia")):
        for l in linhas:
            l[pos] = 1 + sum(1 for x in linhas if x[chave] > l[chave])
    return sorted([l for l in linhas if l["posicao_sem_guia"] <= 10 or l["posicao_com_guia"] <= 10],
                  key=lambda l: (l["posicao_sem_guia"], l["subprefeitura"]))


def q2(ouro):
    por_cat = defaultdict(list)
    for o in ouro["fato_ocorrencia"]:
        if o["origem"] == "sp156" and o["status"] == "finalizada" and o["dias_ate_conclusao"] is not None:
            por_cat[o["categoria"]].append(o["dias_ate_conclusao"])
    return [{"categoria": c, "finalizadas": len(v), "media_dias": arred(statistics.mean(v)),
             "mediana_dias": arred(statistics.median(v))} for c, v in sorted(por_cat.items())]


def q3(ouro):
    por = defaultdict(Counter)
    for o in ouro["fato_ocorrencia"]:
        por[o["origem"]][o["status"]] += 1
    saida = []
    for origem in sorted(por):
        total = sum(por[origem].values())
        for st in ("finalizada", "indeferida", "cancelada", "em_aberto"):
            saida.append({"origem": origem, "status": st, "solicitacoes": por[origem][st],
                          "percentual": arred(100 * por[origem][st] / total)})
    return saida


def q4(ouro, com_guia):
    ini, fim = PERIODO_Q4
    atrop = Counter(s["subprefeitura"] for s in ouro["fato_sinistro_pedestre"] if ini <= s["data"] <= fim)
    recl = Counter(o["subprefeitura"] for o in ouro["fato_ocorrencia"]
                   if o["origem"] == "sp156" and ini <= o["data"] <= fim and (com_guia or o["categoria"] == "calcada"))
    linhas = [{"subprefeitura": s, "atropelamentos": atrop[s], "reclamacoes": recl[s]} for s in subprefeituras(ouro)]
    p_a = percentil([l["atropelamentos"] for l in linhas], 0.75)
    p_r = percentil([l["reclamacoes"] for l in linhas], 0.75)
    for l in linhas:
        l["quartil_superior_nas_duas"] = l["atropelamentos"] >= p_a and l["reclamacoes"] >= p_r
    return linhas, p_a, p_r


def q5(ouro):
    sp = [o for o in ouro["fato_ocorrencia"] if o["origem"] == "sp156"]
    ini, fim = min(o["data"] for o in sp), max(o["data"] for o in sp)
    por_dia = Counter(o["data"] for o in sp)
    grupos = defaultdict(list)
    for d, t in ouro["dim_tempo"].items():
        if ini <= d <= fim and t["precipitacao_dia_mm"] is not None:
            grupos["chuva_forte" if t["chuva_forte"] else "demais_dias"].append(por_dia[d])
    return [{"grupo": g, "dias": len(v), "solicitacoes": sum(v), "media_diaria": arred(statistics.mean(v), 1)}
            for g, v in sorted(grupos.items())], (ini, fim)


def vw_prioridades(ouro):
    """Mesmas colunas e regras da view workspace.gs.vw_prioridades (sem atualizado_em)."""
    fo = ouro["fato_ocorrencia"]
    linhas = {}
    for sub in subprefeituras(ouro):
        sp = [o for o in fo if o["origem"] == "sp156" and o["subprefeitura"] == sub]
        rel = [o for o in fo if o["origem"] == "relato" and o["status"] == "em_aberto" and o["subprefeitura"] == sub]
        sin = [x for x in ouro["fato_sinistro_pedestre"] if x["subprefeitura"] == sub]
        dias = [o["dias_ate_conclusao"] for o in sp if o["status"] == "finalizada" and o["dias_ate_conclusao"] is not None]
        linhas[sub] = {
            "subprefeitura": sub,
            "relatos_abertos": len(rel),
            "relatos_por_categoria": {k: sum(1 for o in rel if o["categoria"] == k) for k in CATEGORIAS_RELATO},
            "tempo_medio_ate_reparo_dias": arred(statistics.mean(dias)) if dias else None,
            "reclamacoes_calcada_sp156": sum(1 for o in sp if o["categoria"] == "calcada"),
            "reclamacoes_guia_sarjeta_sp156": sum(1 for o in sp if o["categoria"] == "guia_sarjeta"),
            "atropelamentos_pedestres": len(sin),
            "pedestres_mortos": sum(x["qtd_pedestres_mortos"] or 0 for x in sin),
            "pct_em_aberto": arred(100 * sum(1 for o in sp if o["status"] == "em_aberto") / len(sp)) if sp else None,
        }

    def mm(chave, v):
        valores = [l[chave] for l in linhas.values()]
        return 0 if max(valores) == min(valores) else (v - min(valores)) / (max(valores) - min(valores))

    for l in linhas.values():
        l["_indice"] = (mm("atropelamentos_pedestres", l["atropelamentos_pedestres"])
                        + mm("reclamacoes_calcada_sp156", l["reclamacoes_calcada_sp156"])) / 2
    n = len(linhas)
    for l in linhas.values():
        posicao = sum(1 for x in linhas.values() if x["_indice"] < l["_indice"]) / (n - 1)  # PERCENT_RANK
        l["prioridade"] = "alta" if posicao >= 2 / 3 else "media" if posicao >= 1 / 3 else "baixa"
        l["indice_prioridade"] = arred(l["_indice"], 3)
    return sorted(linhas.values(), key=lambda l: (-l["_indice"], l["subprefeitura"]))


def mostrar(titulo, linhas):
    print(f"\n### {titulo}")
    if not linhas:
        print("(vazio)")
        return
    cols = list(linhas[0])
    larg = {c: max(len(c), *(len(str(l[c])) for l in linhas)) for c in cols}
    print(" | ".join(c.ljust(larg[c]) for c in cols))
    print("-+-".join("-" * larg[c] for c in cols))
    for l in linhas:
        print(" | ".join(str(l[c]).ljust(larg[c]) for c in cols))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--corte", default="2026-08-31")
    corte = date.fromisoformat(ap.parse_args().corte)
    ouro = montar_ouro(corte)
    dg = ouro["diag"]
    print(f"Corte: {corte}. Linhas na Prata (refeita aqui): {ouro['prata']}")
    print("Barrados:", {k: dict(v) for k, v in dg["rejeitados"].items()})
    fo = ouro["fato_ocorrencia"]
    print("fato_ocorrencia:", Counter(o["origem"] for o in fo), "| fato_sinistro_pedestre:", len(ouro["fato_sinistro_pedestre"]),
          "| dim_tempo:", len(ouro["dim_tempo"]))
    print("status sp156:", dict(Counter(o["status"] for o in fo if o["origem"] == "sp156")),
          "| status relatos:", dict(Counter(o["status"] for o in fo if o["origem"] == "relato")))
    print("Atribuição: pontos", dg["pontos"], "| caíram em fresta (vizinho <=50 m):", dg["vizinho"],
          "| sub x pai do distrito divergem:", dg["sub_x_pai_distrito_diverge"])
    print("SP156 subprefeitura declarada inválida (vai para o ponto):", dict(dg["sp156_decl_sub_invalida"]),
          "| distrito inválido:", dg["sp156_decl_dis_invalida"])
    print(f"SP156 subprefeitura declarada válida: {dg['sp156_sub_validas']}, das quais {dg['sp156_sub_diverge']} "
          f"({100 * dg['sp156_sub_diverge'] / dg['sp156_sub_validas']:.1f}%) divergem do ponto no polígono e "
          f"{dg['sp156_par_incoerente']} contradizem o próprio distrito declarado; "
          f"pontos fora de qualquer polígono resolvidos pelo declarado: {dg['sp156_reserva']}")
    print("local NAO_IDENTIFICADO: ", Counter(o["origem"] for o in fo if o["subprefeitura"] == NAO_IDENTIFICADO),
          "sinistros:", sum(1 for s in ouro["fato_sinistro_pedestre"] if s["subprefeitura"] == NAO_IDENTIFICADO))
    mostrar("Q1", q1(ouro))
    mostrar("Q2", q2(ouro))
    mostrar("Q3", q3(ouro))
    for com in (False, True):
        linhas, pa, pr = q4(ouro, com)
        print(f"\n(P75 atropelamentos={pa}, P75 reclamações={pr})")
        mostrar(f"Q4 {'com' if com else 'sem'} guia e sarjeta (só as marcadas)", [l for l in linhas if l["quartil_superior_nas_duas"]])
    q, (ini, fim) = q5(ouro)
    print(f"\n(período em comum SP156 x clima: {ini} a {fim}; limiar {LIMIAR_CHUVA_FORTE_MM} mm/dia)")
    mostrar("Q5", q)
    vw = vw_prioridades(ouro)
    mostrar("vw_prioridades (colunas do contrato e de análise; relatos_por_categoria resumido)", [
        {"subprefeitura": l["subprefeitura"], "prioridade": l["prioridade"], "indice": l["indice_prioridade"],
         "calcada": l["reclamacoes_calcada_sp156"], "guia_sarj": l["reclamacoes_guia_sarjeta_sp156"],
         "atropel": l["atropelamentos_pedestres"], "mortos": l["pedestres_mortos"],
         "dias_medio": l["tempo_medio_ate_reparo_dias"], "pct_aberto": l["pct_em_aberto"],
         "rel_abertos": l["relatos_abertos"], "rel_por_cat": tuple(l["relatos_por_categoria"].values())} for l in vw])
    print("Faixas:", dict(Counter(l["prioridade"] for l in vw)))


if __name__ == "__main__":
    main()
