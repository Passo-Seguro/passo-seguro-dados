"""Baixa os limites de subprefeituras e distritos do GeoSampa (WFS oficial) e gera os GeoJSON leves da Ouro.

Fonte: GeoSampa / SMUL (Prefeitura de São Paulo), serviço WFS público, sem login.
Saída (em entrada_manual/geo/, que não vai para o Git):
  bruto/subprefeitura.geojson, bruto/distrito.geojson   -> como o serviço devolveu (EPSG:4326)
  subprefeitura.geojson, distrito.geojson               -> simplificados; são estes que sobem ao Databricks

Uso:  python scripts/preparar_geo.py
Se o serviço recusar o acesso (401/403) o script PARA; ele não tenta contornar.
"""
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path

import shapely
from shapely.geometry import mapping, shape
from shapely.validation import make_valid

WFS = "http://wfs.geosampa.prefeitura.sp.gov.br/geoserver/geoportal/wfs"
CAMADAS = {
    # arquivo: (camada WFS, nº esperado de polígonos, propriedades que ficam)
    "subprefeitura": (
        "geoportal:subprefeitura",
        32,
        ["cd_identificador_subprefeitura", "cd_subprefeitura", "nm_subprefeitura", "sg_subprefeitura"],
    ),
    "distrito": (
        "geoportal:distrito_municipal",
        96,
        ["cd_identificador_distrito", "cd_identificador_subprefeitura", "cd_distrito_municipal",
         "nm_distrito_municipal", "sg_distrito_municipal"],
    ),
}
# Mesma caixa do município usada na regra de qualidade da Prata
LAT_MIN, LAT_MAX = -24.01, -23.35
LON_MIN, LON_MAX = -46.83, -46.36
# Tolerância de simplificação em graus (~5,5 m)
TOLERANCIA = 0.00005
CASAS = 5  # ~1 m

RAIZ = Path(__file__).resolve().parent.parent
DESTINO = RAIZ / "entrada_manual" / "geo"


def baixar(camada):
    params = {
        "service": "WFS", "version": "2.0.0", "request": "GetFeature", "typeNames": camada,
        "outputFormat": "application/json", "srsName": "EPSG:4326",
    }
    url = f"{WFS}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=180) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            sys.exit(f"PARE: o serviço recusou o acesso ({e.code}) em {camada}. Não vou tentar contornar.")
        raise


def percorrer(coords):
    if isinstance(coords[0], (int, float)):
        yield coords
    else:
        for c in coords:
            yield from percorrer(c)


def validar(nome, colecao, esperado):
    feicoes = colecao["features"]
    pts = list(percorrer([f["geometry"]["coordinates"] for f in feicoes]))
    lons, lats = [p[0] for p in pts], [p[1] for p in pts]
    dentro = LAT_MIN <= min(lats) and max(lats) <= LAT_MAX and LON_MIN <= min(lons) and max(lons) <= LON_MAX
    print(f"[{nome}] {len(feicoes)} polígonos (esperado {esperado}); lat {min(lats):.4f}..{max(lats):.4f}, "
          f"lon {min(lons):.4f}..{max(lons):.4f}; dentro da caixa de SP: {dentro}")
    if len(feicoes) != esperado or not dentro:
        sys.exit(f"PARE: {nome} fora do esperado.")


def so_poligonos(g):
    """make_valid pode devolver GeometryCollection (polígonos + linhas soltas): fica só a parte poligonal."""
    if g.geom_type in ("Polygon", "MultiPolygon"):
        return g
    partes = []
    for p in g.geoms:
        if p.geom_type == "Polygon":
            partes.append(p)
        elif p.geom_type == "MultiPolygon":
            partes.extend(p.geoms)
    return shapely.MultiPolygon(partes)


def simplificar(colecao, propriedades):
    # A fonte traz alguns polígonos com auto-interseção; make_valid conserta sem mudar a forma
    geoms = [so_poligonos(make_valid(shape(f["geometry"]))) for f in colecao["features"]]
    # Polígono a polígono, preservando a topologia de cada um. Pode nascer fresta de poucos metros entre
    # vizinhos; a Ouro cobre isso atribuindo o polígono mais próximo (até 50 m) a um ponto que cair na fresta.
    leves = [g.simplify(TOLERANCIA, preserve_topology=True) for g in geoms]
    saida = []
    for f, g in zip(colecao["features"], leves):
        g = shapely.set_precision(g, 10 ** -CASAS)  # arredonda as coordenadas
        saida.append({
            "type": "Feature",
            "properties": {k: f["properties"].get(k) for k in propriedades},
            "geometry": mapping(g),
        })
    return {"type": "FeatureCollection", "features": saida}


def main():
    (DESTINO / "bruto").mkdir(parents=True, exist_ok=True)
    print("Fonte: GeoSampa/SMUL, WFS", WFS, "| baixado em", date.today().isoformat())
    for nome, (camada, esperado, propriedades) in CAMADAS.items():
        bruto = baixar(camada)
        validar(nome, bruto, esperado)
        (DESTINO / "bruto" / f"{nome}.geojson").write_text(json.dumps(bruto, ensure_ascii=False), encoding="utf-8")
        leve = simplificar(bruto, propriedades)
        arquivo = DESTINO / f"{nome}.geojson"
        arquivo.write_text(json.dumps(leve, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        tam_bruto = (DESTINO / "bruto" / f"{nome}.geojson").stat().st_size / 1024
        print(f"[{nome}] simplificado: {tam_bruto:,.0f} KB -> {arquivo.stat().st_size / 1024:,.0f} KB")


if __name__ == "__main__":
    main()
