from flask import Flask, render_template, jsonify, request, send_file
import pandas as pd
import geopandas as gpd
import json
from io import BytesIO

app = Flask(__name__)

# Carregar CSV
df = pd.read_csv('precipitacao.csv')

# Corrigir separadores decimais
df['Lat'] = df['Lat'].str.replace(',', '.').astype(float)
df['Long'] = df['Long'].str.replace(',', '.').astype(float)

# Criar uma lista única de estados e datas
ufs = sorted(df['SIGLA_UF'].unique())
datas = sorted(df['date'].unique())

@app.route('/')
def index():
    return render_template('index.html', ufs=ufs, datas=datas)

@app.route('/data')
def data():
    uf = request.args.get('uf')
    data_filtro = request.args.get('data')

    filtro = df.copy()

    if uf:
        filtro = filtro[filtro['SIGLA_UF'] == uf]
    if data_filtro:
        filtro = filtro[filtro['date'] == data_filtro]

    gdf = gpd.GeoDataFrame(
        filtro,
        geometry=gpd.points_from_xy(filtro['Long'], filtro['Lat']),
        crs='EPSG:4326'
    )

    geojson = json.loads(gdf.to_json())

    return jsonify(geojson)

@app.route('/download')
def download():
    uf = request.args.get('uf')
    data_filtro = request.args.get('data')

    filtro = df.copy()

    if uf:
        filtro = filtro[filtro['SIGLA_UF'] == uf]
    if data_filtro:
        filtro = filtro[filtro['date'] == data_filtro]

    output = BytesIO()
    filtro.to_csv(output, index=False)
    output.seek(0)

    return send_file(
        output,
        mimetype='text/csv',
        download_name='dados_filtrados.csv',
        as_attachment=True
    )

if __name__ == '__main__':
    app.run(debug=True)
