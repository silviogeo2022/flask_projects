from flask import Flask, render_template, request
from sqlalchemy import create_engine, text
import pandas as pd
import folium
from folium.plugins import MarkerCluster

# Conexão Postgres (Render)
DATABASE_URL = (
    "postgresql+psycopg2://root:"
    "LbscwgBgBGGXk5GFrfFAWK23EkRxjrre"
    "@dpg-d398e6ffte5s73ckr9vg-a.oregon-postgres.render.com:5432/"
    "urbano_mdr?sslmode=require"
)

TABLE = "urbano.solicitacoes"  # ajuste se necessário (schema.tabela)

app = Flask(__name__)
engine = create_engine(DATABASE_URL, pool_pre_ping=True)


def fetch_df(bairro=None, rua=None, situacao=None):
    clauses = ["latitude IS NOT NULL", "longitude IS NOT NULL"]
    params = {}
    if bairro:
        clauses.append("bairro = :bairro")
        params["bairro"] = bairro
    if rua:
        clauses.append("nome_rua = :rua")
        params["rua"] = rua
    if situacao:
        clauses.append("situacoes = :situacao")
        params["situacao"] = situacao

    where_sql = " AND ".join(clauses)
    sql = f"""
        SELECT
            nome_rua,
            bairro,
            latitude::float AS latitude,
            longitude::float AS longitude,
            COALESCE(situacoes, 'Não informado') AS situacoes
        FROM {TABLE}
        WHERE {where_sql}
    """
    with engine.begin() as conn:
        df = pd.read_sql(text(sql), conn, params=params)
    return df


def fetch_distinct(column, bairro=None, rua=None):
    clauses = ["1=1"]
    params = {}
    if bairro and column != "bairro":
        clauses.append("bairro = :bairro")
        params["bairro"] = bairro
    if rua and column != "nome_rua":
        clauses.append("nome_rua = :rua")
        params["rua"] = rua

    sql = f"""
        SELECT DISTINCT {column} AS v
        FROM {TABLE}
        WHERE {' AND '.join(clauses)} AND {column} IS NOT NULL AND {column} <> ''
        ORDER BY 1
    """
    with engine.begin() as conn:
        rows = conn.execute(text(sql), params).fetchall()
    return [r[0] for r in rows]


def add_basemaps(m, selected="satellite"):
    order = ["satellite", "positron", "osm"]#satellit
    if selected in order:
        order.remove(selected)
        order.append(selected)

    for key in order:
        if key == "satellite":
            folium.TileLayer("OpenStreetMap", name="Padrão").add_to(m)
        elif key == "positron":
            folium.TileLayer("CartoDB positron", name="Claro").add_to(m)
        elif key == "osm":
            folium.TileLayer(
                tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
                attr="Esri World Imagery",
                name="Satélite (Esri)",
                overlay=False,
                control=True,
            ).add_to(m)


@app.route("/", methods=["GET"])
def index():
    # Filtros
    bairro = request.args.get("bairro") or None
    rua = request.args.get("rua") or None
    situacao = request.args.get("situacao") or None
    basemap = request.args.get("basemap") or "osm"

    # Dados
    df = fetch_df(bairro=bairro, rua=rua, situacao=situacao)
    total = int(len(df))

    bairros = fetch_distinct("bairro")
    ruas = fetch_distinct("nome_rua", bairro=bairro)
    situacoes = fetch_distinct("situacoes", bairro=bairro, rua=rua)

    # Stats para o painel
    by_situacao = (df["situacoes"].fillna("Não informado").value_counts().to_dict()
                   if not df.empty else {})
    num_situacoes = len(by_situacao)
    if situacao:
        indicador_texto = f"{total} registro(s) da situação '{situacao}'"
    elif bairro and not rua:
        indicador_texto = f"{total} registro(s) em '{bairro}' • Situações distintas: {num_situacoes}"
    else:
        indicador_texto = f"{total} registro(s) filtrado(s)"
    stats = {
        "total_registros": total,
        "por_situacao": by_situacao,
        "num_situacoes": num_situacoes,
        "indicador_texto": indicador_texto,
    }

    # Prepara bounds antes de criar o mapa (para garantir o zoom correto)
    bounds = []
    if total > 0:
        bounds = [[float(r.latitude), float(r.longitude)] for r in df.itertuples()]

    # Define centro/zoom inicial com base nos filtros
    default_center = [-2.049924, -47.551264]
    default_zoom = 5 #5
    if total > 0:
        if len(bounds) == 1:
            initial_center = bounds[0]
            initial_zoom = 16  # foco em um único ponto 16
        else:
            # centro médio; fit_bounds ajustará depois
            initial_center = [float(df.latitude.mean()), float(df.longitude.mean())]
            initial_zoom = 12 if (bairro or rua or situacao) else 6
    else:
        initial_center, initial_zoom = default_center, default_zoom

    # Construção do mapa
    m = folium.Map(location=initial_center, zoom_start=initial_zoom,
                   control_scale=True, tiles=None)
    add_basemaps(m, selected=basemap)

    # Marcadores
    if total > 0:
        mc = MarkerCluster(name="Registros").add_to(m)
        for _, row in df.iterrows():
            lat, lon = float(row["latitude"]), float(row["longitude"])
            popup_html = f"""
                <b>Bairro:</b> {row['bairro']}<br>
                <b>Rua:</b> {row['nome_rua']}<br>
                <b>Situação:</b> {row['situacoes']}
            """
            folium.Marker(
                [lat, lon],
                tooltip=row["nome_rua"] or "Ponto",
                popup=folium.Popup(popup_html, max_width=320),
                icon=folium.Icon(color="blue", icon="info-sign"),
            ).add_to(mc)

        # Ajusta o zoom para cobrir exatamente os pontos filtrados
        if len(bounds) > 1:
            m.fit_bounds(bounds)  # cobre todos os pontos filtrados

    folium.LayerControl().add_to(m)
    map_html = m._repr_html_()

    return render_template(
        "dashboard.html",
        map_html=map_html,
        total=total,
        bairros=bairros,
        ruas=ruas,
        situacoes=situacoes,
        selected_bairro=bairro or "",
        selected_rua=rua or "",
        selected_situacao=situacao or "",
        basemap=basemap,
        stats=stats,
    )


if __name__ == "__main__":
    # pip install flask sqlalchemy psycopg2-binary pandas folium
    app.run(host="0.0.0.0", port=5000, debug=True)