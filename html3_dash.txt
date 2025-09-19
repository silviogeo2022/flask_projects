from flask import Flask, render_template, jsonify, request, send_file
import pandas as pd
import geopandas as gpd
import json
from io import BytesIO
import logging
from datetime import datetime, timedelta
import numpy as np
from functools import lru_cache
import os

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-key-change-in-production')
app.config['DEBUG'] = os.environ.get('DEBUG', 'True').lower() == 'true'

# Cache para otimização
@lru_cache(maxsize=128)
def load_and_process_data():
    """Carrega e processa os dados com cache"""
    try:
        df = pd.read_csv('precipitacao.csv')
        
        # Corrigir separadores decimais
        df['Lat'] = df['Lat'].str.replace(',', '.').astype(float)
        df['Long'] = df['Long'].str.replace(',', '.').astype(float)
        
        # Converter precipitation para float, tratando valores não numéricos
        df['precipitation'] = pd.to_numeric(df['precipitation'], errors='coerce')
        
        # Remover linhas com coordenadas ou precipitação inválidas
        df = df.dropna(subset=['Lat', 'Long', 'precipitation'])
        
        logger.info(f"Dados carregados: {len(df)} registros")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame()

# Carregar dados
df = load_and_process_data()

# Validar se os dados foram carregados
if df.empty:
    logger.warning("Nenhum dado foi carregado. Usando dados de exemplo.")
    # Criar dados de exemplo para demonstração
    df = pd.DataFrame({
        'SIGLA_UF': ['SP', 'RJ', 'MG'] * 10,
        'NM_MUN': [f'Cidade_{i}' for i in range(30)],
        'Lat': np.random.uniform(-30, -10, 30),
        'Long': np.random.uniform(-55, -35, 30),
        'precipitation': np.random.uniform(0, 150, 30),
        'date': ['2024-01-01', '2024-01-02', '2024-01-03'] * 10
    })

# Criar listas únicas de estados e datas
ufs = sorted(df['SIGLA_UF'].unique())
datas = sorted(df['date'].unique())

def validate_filters(uf=None, data=None, min_precip=None, max_precip=None):
    """Valida os filtros recebidos"""
    errors = []
    
    if uf and uf not in ufs:
        errors.append(f"Estado '{uf}' não encontrado")
    
    if data and data not in datas:
        errors.append(f"Data '{data}' não encontrada")
    
    if min_precip is not None:
        try:
            min_precip = float(min_precip)
            if min_precip < 0:
                errors.append("Precipitação mínima deve ser >= 0")
        except ValueError:
            errors.append("Precipitação mínima deve ser um número")
    
    if max_precip is not None:
        try:
            max_precip = float(max_precip)
            if max_precip < 0:
                errors.append("Precipitação máxima deve ser >= 0")
        except ValueError:
            errors.append("Precipitação máxima deve ser um número")
    
    return errors

@app.route('/')
def index():
    """Página principal"""
    try:
        stats = get_basic_stats()
        return render_template('index.html', 
                             ufs=ufs, 
                             datas=datas,
                             stats=stats)
    except Exception as e:
        logger.error(f"Erro na página principal: {e}")
        return render_template('index.html', 
                             ufs=[], 
                             datas=[],
                             stats={})

@app.route('/data')
def data():
    """Endpoint para dados filtrados"""
    try:
        # Obter parâmetros
        uf = request.args.get('uf')
        data_filtro = request.args.get('data')
        min_precip = request.args.get('min_precip')
        max_precip = request.args.get('max_precip')
        municipios = request.args.getlist('municipios')  # Para múltiplos municípios
        
        # Validar filtros
        errors = validate_filters(uf, data_filtro, min_precip, max_precip)
        if errors:
            return jsonify({'error': errors}), 400
        
        # Aplicar filtros
        filtro = df.copy()
        
        if uf:
            filtro = filtro[filtro['SIGLA_UF'] == uf]
        
        if data_filtro:
            filtro = filtro[filtro['date'] == data_filtro]
        
        if min_precip is not None:
            filtro = filtro[filtro['precipitation'] >= float(min_precip)]
        
        if max_precip is not None:
            filtro = filtro[filtro['precipitation'] <= float(max_precip)]
        
        if municipios:
            filtro = filtro[filtro['NM_MUN'].isin(municipios)]
        
        # Verificar se há dados após filtros
        if filtro.empty:
            return jsonify({
                'type': 'FeatureCollection',
                'features': [],
                'message': 'Nenhum dado encontrado com os filtros aplicados'
            })
        
        # Criar GeoDataFrame
        gdf = gpd.GeoDataFrame(
            filtro,
            geometry=gpd.points_from_xy(filtro['Long'], filtro['Lat']),
            crs='EPSG:4326'
        )
        
        # Converter para GeoJSON
        geojson = json.loads(gdf.to_json())
        
        logger.info(f"Dados filtrados: {len(filtro)} registros")
        return jsonify(geojson)
        
    except Exception as e:
        logger.error(f"Erro ao filtrar dados: {e}")
        return jsonify({'error': 'Erro interno do servidor'}), 500

@app.route('/stats')
def stats():
    """Endpoint para estatísticas"""
    try:
        uf = request.args.get('uf')
        data_filtro = request.args.get('data')
        
        # Aplicar filtros básicos
        filtro = df.copy()
        
        if uf:
            filtro = filtro[filtro['SIGLA_UF'] == uf]
        if data_filtro:
            filtro = filtro[filtro['date'] == data_filtro]
        
        if filtro.empty:
            return jsonify({'error': 'Nenhum dado encontrado'}), 404
        
        # Calcular estatísticas
        stats_data = {
            'total_registros': len(filtro),
            'precipitacao_media': float(filtro['precipitation'].mean()),
            'precipitacao_maxima': float(filtro['precipitation'].max()),
            'precipitacao_minima': float(filtro['precipitation'].min()),
            'precipitacao_total': float(filtro['precipitation'].sum()),
            'municipios_unicos': len(filtro['NM_MUN'].unique()),
            'estados_unicos': len(filtro['SIGLA_UF'].unique()),
            'por_estado': filtro.groupby('SIGLA_UF')['precipitation'].agg(['mean', 'sum', 'count']).to_dict(),
            'distribuicao_faixas': {
                'baixa (0-10mm)': len(filtro[filtro['precipitation'] <= 10]),
                'moderada (10-30mm)': len(filtro[(filtro['precipitation'] > 10) & (filtro['precipitation'] <= 30)]),
                'alta (30-70mm)': len(filtro[(filtro['precipitation'] > 30) & (filtro['precipitation'] <= 70)]),
                'muito_alta (70mm+)': len(filtro[filtro['precipitation'] > 70])
            }
        }
        
        return jsonify(stats_data)
        
    except Exception as e:
        logger.error(f"Erro ao calcular estatísticas: {e}")
        return jsonify({'error': 'Erro ao calcular estatísticas'}), 500

@app.route('/timeline')
def timeline():
    """Dados para gráfico de timeline"""
    try:
        uf = request.args.get('uf')
        
        filtro = df.copy()
        if uf:
            filtro = filtro[filtro['SIGLA_UF'] == uf]
        
        # Agrupar por data
        timeline_data = filtro.groupby('date')['precipitation'].agg(['mean', 'sum', 'count']).reset_index()
        timeline_data.columns = ['data', 'precipitacao_media', 'precipitacao_total', 'num_registros']
        
        return jsonify(timeline_data.to_dict(orient='records'))
        
    except Exception as e:
        logger.error(f"Erro ao gerar timeline: {e}")
        return jsonify({'error': 'Erro ao gerar timeline'}), 500

@app.route('/municipios')
def municipios():
    """Lista municípios para autocomplete"""
    try:
        uf = request.args.get('uf')
        termo = request.args.get('q', '').lower()
        
        filtro = df.copy()
        if uf:
            filtro = filtro[filtro['SIGLA_UF'] == uf]
        
        municipios_list = filtro['NM_MUN'].unique().tolist()
        
        # Filtrar por termo de busca
        if termo:
            municipios_list = [m for m in municipios_list if termo in m.lower()]
        
        return jsonify(sorted(municipios_list)[:50])  # Limitar a 50 resultados
        
    except Exception as e:
        logger.error(f"Erro ao listar municípios: {e}")
        return jsonify([])

@app.route('/download')
def download():
    """Download de dados com melhorias"""
    try:
        # Obter parâmetros
        uf = request.args.get('uf')
        data_filtro = request.args.get('data')
        formato = request.args.get('format', 'csv').lower()
        min_precip = request.args.get('min_precip')
        max_precip = request.args.get('max_precip')
        
        # Aplicar filtros
        filtro = df.copy()
        
        if uf:
            filtro = filtro[filtro['SIGLA_UF'] == uf]
        if data_filtro:
            filtro = filtro[filtro['date'] == data_filtro]
        if min_precip:
            filtro = filtro[filtro['precipitation'] >= float(min_precip)]
        if max_precip:
            filtro = filtro[filtro['precipitation'] <= float(max_precip)]
        
        if filtro.empty:
            return jsonify({'error': 'Nenhum dado para download'}), 404
        
        output = BytesIO()
        
        # Diferentes formatos de export
        if formato == 'json':
            filtro.to_json(output, orient='records', date_format='iso')
            mimetype = 'application/json'
            filename = 'dados_precipitacao.json'
        elif formato == 'excel':
            filtro.to_excel(output, index=False, engine='openpyxl')
            mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            filename = 'dados_precipitacao.xlsx'
        else:  # CSV padrão
            filtro.to_csv(output, index=False, encoding='utf-8-sig')
            mimetype = 'text/csv'
            filename = 'dados_precipitacao.csv'
        
        output.seek(0)
        
        logger.info(f"Download realizado: {len(filtro)} registros em formato {formato}")
        
        return send_file(
            output,
            mimetype=mimetype,
            download_name=filename,
            as_attachment=True
        )
        
    except Exception as e:
        logger.error(f"Erro no download: {e}")
        return jsonify({'error': 'Erro ao gerar download'}), 500

@app.route('/heatmap')
def heatmap():
    """Dados para heatmap"""
    try:
        uf = request.args.get('uf')
        data_filtro = request.args.get('data')
        
        filtro = df.copy()
        
        if uf:
            filtro = filtro[filtro['SIGLA_UF'] == uf]
        if data_filtro:
            filtro = filtro[filtro['date'] == data_filtro]
        
        # Preparar dados para heatmap (lat, lng, intensity)
        heatmap_data = filtro[['Lat', 'Long', 'precipitation']].values.tolist()
        
        return jsonify(heatmap_data)
        
    except Exception as e:
        logger.error(f"Erro ao gerar heatmap: {e}")
        return jsonify([])

def get_basic_stats():
    """Estatísticas básicas para a página inicial"""
    try:
        return {
            'total_registros': len(df),
            'total_municipios': len(df['NM_MUN'].unique()),
            'total_estados': len(df['SIGLA_UF'].unique()),
            'precipitacao_media': float(df['precipitation'].mean()) if not df.empty else 0,
            'periodo': {
                'inicio': min(datas) if datas else None,
                'fim': max(datas) if datas else None
            }
        }
    except:
        return {}

# Tratamento de erros
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint não encontrado'}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Erro interno: {error}")
    return jsonify({'error': 'Erro interno do servidor'}), 500

@app.errorhandler(400)
def bad_request(error):
    return jsonify({'error': 'Requisição inválida'}), 400

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=app.config['DEBUG'])