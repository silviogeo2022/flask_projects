import json
from pathlib import Path
from functools import lru_cache

from flask import Flask, render_template, request
import pandas as pd
import plotly.express as px
import folium

app = Flask(__name__)

# Caminho do GeoJSON (na mesma pasta do script)
DATA_PATH = Path(__file__).parent / "BD_CONSUMO_AGUA_AC.geojson"

# Nome do template principal
TEMPLATE_INDEX = "index_agua.html"


# -----------------------
# UTILITÁRIOS PARA GEOJSON
# -----------------------
def _is_git_lfs_pointer(txt_head: str) -> bool:
    return "git-lfs.github.com/spec" in txt_head


def _iter_coords(geom):
    """Percorre coordenadas de vários tipos de geometria do GeoJSON."""
    if not geom:
        return
    t = geom.get("type")
    coords = geom.get("coordinates")
    if t == "Point":
        yield coords
    elif t in ("MultiPoint", "LineString"):
        for c in coords:
            yield c
    elif t in ("MultiLineString", "Polygon"):
        for part in coords:
            for c in part:
                yield c
    elif t == "MultiPolygon":
        for poly in coords:
            for ring in poly:
                for c in ring:
                    yield c
    elif t == "GeometryCollection":
        for g in geom.get("geometries", []):
            yield from _iter_coords(g)


def _bounds_from_geojson(gj: dict):
    """Retorna (minx, miny, maxx, maxy) do FeatureCollection."""
    xs, ys = [], []
    for feat in gj.get("features", []):
        for x, y in _iter_coords(feat.get("geometry")):
            xs.append(x)
            ys.append(y)
    if not xs or not ys:
        return None
    return min(xs), min(ys), max(xs), max(ys)


def _filter_geojson(gj: dict, area: str, bairros_sel: list):
    """Filtra o FeatureCollection considerando campos ausentes como 'Não informado'."""

    def norm(v, default="Não informado"):
        if v is None:
            return default
        if isinstance(v, str) and v.strip() in ("", "nan", "None"):
            return default
        return v

    def cond(props):
        area_val = norm(props.get("AREA_y"))
        bairro_val = norm(props.get("BAIRRO_COM"))
        if area and area != "Todas" and area_val != area:
            return False
        if bairros_sel and bairro_val not in bairros_sel:
            return False
        return True

    feats = [f for f in gj.get("features", []) if cond(f.get("properties", {}))]
    return {"type": "FeatureCollection", "features": feats}


# -----------------------
# CARREGAMENTO DE DADOS (com cache em memória)
# -----------------------
@lru_cache(maxsize=1)
def load_data():
    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {DATA_PATH.name}. "
            "Coloque o BD_CONSUMO_AGUA_AC.geojson na raiz do app."
        )

    head = DATA_PATH.read_text(encoding="utf-8", errors="ignore")[:200]
    if _is_git_lfs_pointer(head):
        raise RuntimeError(
            "O GeoJSON parece ser um 'pointer' do Git LFS. "
            "Remova do LFS e faça commit do arquivo real no Git."
        )

    with DATA_PATH.open("r", encoding="utf-8") as f:
        gj = json.load(f)

    # DataFrame com as propriedades dos features
    props_list = [feat.get("properties", {}) for feat in gj.get("features", [])]
    df = pd.DataFrame(props_list)

    # Garante colunas usadas
    text_cols = ["AREA_y", "BAIRRO_COM", "LABEL_Q5", "LABEL_Q6", "LABEL_Q7", "LABEL_Q8", "LABEL_Q9", "LABEL_Q10"]
    for c in text_cols:
        if c not in df.columns:
            df[c] = "Não informado"
        df[c] = df[c].fillna("Não informado").astype(str)

    num_cols = ["N_domi", "Pop_estim1"]
    for c in num_cols:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    return df, gj


def fmt_int(n):
    # Formata com separador de milhar (.) para facilitar leitura
    try:
        return f"{int(n):,}".replace(",", ".")
    except Exception:
        return str(n)


def make_pie_html(dataframe, col, title):
    """Gera HTML do gráfico de pizza (Plotly) e garante o carregamento do plotly.js via CDN."""
    if col not in dataframe.columns:
        return None
    tmp = dataframe.copy()
    tmp[col] = tmp[col].fillna("Não informado").astype(str)
    fig = px.pie(tmp, names=col, title=title)
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    # include_plotlyjs="cdn" injeta a tag <script> do Plotly automaticamente
    return fig.to_html(full_html=False, include_plotlyjs="cdn", config={"displayModeBar": False})


def make_map_html(geojson_filtered):
    """Gera o HTML do mapa Folium com os polígonos filtrados."""
    if not geojson_filtered.get("features"):
        return None

    bounds = _bounds_from_geojson(geojson_filtered)
    if bounds:
        minx, miny, maxx, maxy = bounds
        center = [(miny + maxy) / 2, (minx + maxx) / 2]
    else:
        center = [-5.0, -50.0]  # fallback

    m = folium.Map(location=center, zoom_start=11, tiles="cartodbpositron")

    folium.GeoJson(
        data=geojson_filtered,
        name="Polígonos",
        style_function=lambda x: {
            "fillColor": "#1f78b4",
            "color": "black",
            "weight": 1,
            "fillOpacity": 0.4,
        },
        tooltip=folium.GeoJsonTooltip(
            fields=["AREA_y", "BAIRRO_COM"],
            aliases=["Área:", "Bairro:"],
        ),
    ).add_to(m)

    if bounds:
        m.fit_bounds([[miny, minx], [maxy, maxx]])

    folium.LayerControl().add_to(m)

    # _repr_html_ devolve um HTML embutível (div + script)
    return m._repr_html_()


@app.route("/", methods=["GET"])
def index():
    # Carrega dados (cacheado)
    try:
        df_all, geojson_all = load_data()
    except Exception as e:
        return render_template(TEMPLATE_INDEX, error=str(e))

    # Opções de área
    areas = ["Todas"] + sorted(df_all["AREA_y"].dropna().unique().tolist())

    # Lê filtros da query string
    area = request.args.get("area", "Todas")
    bairros_sel = request.args.getlist("bairros")

    # Bairros disponíveis dependem da área
    if area != "Todas":
        bairros_disponiveis = sorted(
            df_all.loc[df_all["AREA_y"] == area, "BAIRRO_COM"].dropna().unique().tolist()
        )
    else:
        bairros_disponiveis = sorted(df_all["BAIRRO_COM"].dropna().unique().tolist())

    # Aplica filtros no DataFrame
    df = df_all.copy()
    if area != "Todas":
        df = df[df["AREA_y"] == area]
    if bairros_sel:
        df = df[df["BAIRRO_COM"].isin(bairros_sel)]

    # Indicadores
    num_domi = fmt_int(df["N_domi"].sum())
    pop_est = fmt_int(df["Pop_estim1"].sum())

    # Gráficos
    charts = {
        "q5": make_pie_html(df, "LABEL_Q5", "Fonte de água de abastecimento"),
        "q8": make_pie_html(df, "LABEL_Q8", "Entrega regular de água"),
        "q7": make_pie_html(df, "LABEL_Q7", "Problemas relacionados à água"),
        "q6": make_pie_html(df, "LABEL_Q6", "Qualidade da água"),
        "q9": make_pie_html(df, "LABEL_Q9", "Falta de água"),
        "q10": make_pie_html(df, "LABEL_Q10", "Poço próximo de fossa séptica"),
    }

    # Mapa
    geojson_filtered = _filter_geojson(geojson_all, area, bairros_sel)
    map_html = make_map_html(geojson_filtered)
    map_warning = None
    if not geojson_filtered.get("features"):
        map_warning = "⚠️ Nenhum polígono encontrado para os filtros selecionados."

    return render_template(
        TEMPLATE_INDEX,
        error=None,
        areas=areas,
        area_selected=area,
        bairros_disponiveis=bairros_disponiveis,
        bairros_selected=bairros_sel,
        num_domi=num_domi,
        pop_est=pop_est,
        charts=charts,
        map_html=map_html,
        map_warning=map_warning,
    )


if __name__ == "__main__":
    # Rode com: python app.py
    app.run(host="0.0.0.0", port=5000, debug=True)