# app.py
import os
import traceback
from decimal import Decimal, InvalidOperation
from datetime import datetime

from flask import Flask, render_template, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import event, text
from sqlalchemy.engine import Engine, URL
from sqlalchemy.sql import quoted_name, func
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key')  # necessário para flash

# ============ Config do Banco ============
DB_USER = os.getenv('PGUSER', 'postgres')
DB_PASSWORD = os.getenv('PGPASSWORD', 'Silvio@2025')     # ajuste se necessário
DB_HOST = os.getenv('PGHOST', 'localhost')
DB_PORT = int(os.getenv('PGPORT', 5432))
DB_NAME = os.getenv('PGDATABASE', 'postgres')
DB_SCHEMA = os.getenv('DB_SCHEMA', 'urbano')

# Em Windows/PT-BR, mensagens do servidor podem vir em CP1252
CLIENT_ENCODING = os.getenv('CLIENT_ENCODING', 'WIN1252')

db_url = URL.create(
    "postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,   # seguro com caracteres especiais
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)

app.config['SQLALCHEMY_DATABASE_URI'] = db_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    "connect_args": {"options": f"-c client_encoding={CLIENT_ENCODING}"}
}
# app.config['SQLALCHEMY_ECHO'] = True  # descomente para ver SQL no console

# ============ Uploads ============
UPLOAD_DIR = os.path.join(app.root_path, 'static', 'uploads')
os.makedirs(UPLOAD_DIR, exist_ok=True)
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
app.config['UPLOAD_FOLDER'] = UPLOAD_DIR
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB

def allowed_file(filename: str) -> bool:
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def parse_coord(value: str):
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    s = s.replace(',', '.')  # aceita vírgula
    try:
        # Numeric(9,6) cabe valores como -123.456789
        return Decimal(s)
    except InvalidOperation:
        return None

db = SQLAlchemy(app)

@event.listens_for(Engine, "connect")
def set_client_encoding(dbapi_connection, connection_record):
    cur = dbapi_connection.cursor()
    try:
        cur.execute(f"SET client_encoding TO '{CLIENT_ENCODING}';")
        try:
            # mensagens neutras em inglês (se permitido)
            cur.execute("SET lc_messages TO 'C';")
        except Exception:
            pass
    finally:
        cur.close()

# ============ Modelo ============
TABLE_NAME = os.getenv('TABLE_NAME', 'solicitacoes')

class Solicitacao(db.Model):
    __tablename__ = quoted_name(TABLE_NAME, True)
    __table_args__ = {'schema': DB_SCHEMA}

    id = db.Column(db.Integer, primary_key=True)
    # mapeia para a coluna existente "nome_rua" (varchar 120)
    rua = db.Column('nome_rua', db.String(120), nullable=False)
    numero = db.Column(db.String(10), nullable=False)     # varchar(10)
    bairro = db.Column(db.String(80), nullable=False)     # varchar(80)
    latitude = db.Column(db.Numeric(9, 6), nullable=True)
    longitude = db.Column(db.Numeric(9, 6), nullable=True)
    foto_path = db.Column(db.Text, nullable=True)
    situacoes = db.Column(db.Text, nullable=True)         # CSV: "buraco,iluminacao"
    criado_em = db.Column('criado_em', db.DateTime(timezone=True), server_default=func.now())

# ============ Rotas ============
@app.route('/')
def index():
    # No template: <meta charset="utf-8"> e <form ... accept-charset="UTF-8" enctype="multipart/form-data">
    return render_template('formulario.html')

@app.route('/enviar', methods=['POST'])
def enviar_formulario():
    nome_rua = (request.form.get('nome_rua') or '').strip()
    numero = (request.form.get('numero') or '').strip()
    bairro = (request.form.get('bairro') or '').strip()
    latitude = parse_coord(request.form.get('latitude'))
    longitude = parse_coord(request.form.get('longitude'))
    situacoes_list = request.form.getlist('situacao')  # múltiplas checkboxes
    situacoes_str = ','.join(situacoes_list) if situacoes_list else None

    # upload da foto (input name="foto")
    foto_file = request.files.get('foto')
    foto_path_rel = None
    if foto_file and foto_file.filename and allowed_file(foto_file.filename):
        filename = secure_filename(foto_file.filename)
        # tornar o nome único
        base, ext = os.path.splitext(filename)
        filename = f"{base}_{int(datetime.now().timestamp())}{ext}"
        destino = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        foto_file.save(destino)
        # caminho relativo/URL para servir via Flask (pasta static)
        foto_path_rel = f"/static/uploads/{filename}"

    if not nome_rua or not numero or not bairro:
        flash('Por favor, preencha nome da rua, número e bairro.', 'error')
        return redirect(url_for('index'))

    try:
        nova = Solicitacao(
            rua=nome_rua,
            numero=str(numero),
            bairro=bairro,
            latitude=latitude,
            longitude=longitude,
            foto_path=foto_path_rel,
            situacoes=situacoes_str
        )
        db.session.add(nova)
        db.session.commit()
        flash('Solicitação enviada com sucesso!', 'success')
    except Exception as e:
        db.session.rollback()
        app.logger.exception("Erro ao salvar no banco")
        flash(f'Erro ao salvar: {type(e).__name__}: {e}', 'error')

    return redirect(url_for('index'))

@app.route('/lista')
def lista():
    itens = Solicitacao.query.order_by(Solicitacao.id.desc()).all()
    linhas = []
    for i in itens:
        loc = f"{i.latitude},{i.longitude}" if (i.latitude is not None and i.longitude is not None) else "-"
        foto = i.foto_path or "-"
        linhas.append(f"{i.id} - {i.rua}, {i.numero} - {i.bairro} | loc: {loc} | foto: {foto} | situações: {i.situacoes or '-'}")
    return '<br>'.join(linhas) or 'Sem registros.'

# Diagnóstico rápido de encoding
@app.route('/debug-enc')
def debug_enc():
    with db.engine.connect() as conn:
        client = conn.execute(text("SHOW client_encoding")).scalar_one()
        server = conn.execute(text("SHOW server_encoding")).scalar_one()
        dbname = conn.execute(text("SELECT current_database()")).scalar_one()
        try:
            lc_messages = conn.execute(text("SHOW lc_messages")).scalar_one()
        except Exception:
            lc_messages = 'desconhecido'
    return f"db={dbname}, client_encoding={client}, server_encoding={server}, lc_messages={lc_messages}, forced={CLIENT_ENCODING}"

if __name__ == '__main__':
    # Garante que schema/tabela/colunas existam
    with app.app_context():
        with db.engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{DB_SCHEMA}"'))
            conn.execute(text(f'''
                CREATE TABLE IF NOT EXISTS "{DB_SCHEMA}"."{TABLE_NAME}" (
                    id BIGSERIAL PRIMARY KEY,
                    nome_rua VARCHAR(120) NOT NULL,
                    numero   VARCHAR(10)  NOT NULL,
                    bairro   VARCHAR(80)  NOT NULL
                )
            '''))
            conn.execute(text(f'''
                ALTER TABLE "{DB_SCHEMA}"."{TABLE_NAME}"
                ADD COLUMN IF NOT EXISTS latitude   NUMERIC(9,6),
                ADD COLUMN IF NOT EXISTS longitude  NUMERIC(9,6),
                ADD COLUMN IF NOT EXISTS foto_path  TEXT,
                ADD COLUMN IF NOT EXISTS situacoes  TEXT,
                ADD COLUMN IF NOT EXISTS criado_em  TIMESTAMPTZ DEFAULT NOW()
            '''))
        db.create_all()

    app.run(debug=True)