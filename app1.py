# app.py
import os
from flask import Flask, render_template, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import event, text
from sqlalchemy.engine import Engine, URL
from sqlalchemy.sql import quoted_name
import traceback

app = Flask(__name__)
#app.secret_key = os.getenv('FLASK_SECRET_KEY', 'sua_chave_secreta_aqui')

# ============ Config do Banco ============
DB_USER = os.getenv('PGUSER', 'postgres')
DB_PASSWORD = os.getenv('PGPASSWORD', 'SUA_SENHA')         # ajuste
DB_HOST = os.getenv('PGHOST', 'localhost')
DB_PORT = int(os.getenv('PGPORT', 5432))
DB_NAME = os.getenv('PGDATABASE', 'formulario_mdr')        # ajuste
DB_SCHEMA = os.getenv('DB_SCHEMA', 'public')

# Em Windows/PT-BR o servidor costuma enviar mensagens em CP1252.
# Para evitar o UnicodeDecodeError 0xe7, começamos com WIN1252.
# Depois que tudo estiver OK, troque para UTF8 se preferir.
CLIENT_ENCODING = os.getenv('CLIENT_ENCODING', 'WIN1252')

db_url = URL.create(
    "postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,   # suporta caracteres especiais e acentos
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)

app.config['SQLALCHEMY_DATABASE_URI'] = db_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    "connect_args": {"options": f"-c client_encoding={CLIENT_ENCODING}"}
}

db = SQLAlchemy(app)

@event.listens_for(Engine, "connect")
def set_client_encoding(dbapi_connection, connection_record):
    cur = dbapi_connection.cursor()
    try:
        cur.execute(f"SET client_encoding TO '{CLIENT_ENCODING}';")
        # Tente neutralizar mensagens que possam vir em CP1252 (pode exigir superuser)
        try:
            cur.execute("SET lc_messages TO 'C';")
        except Exception:
            pass  # ignore se não tiver permissão
    finally:
        cur.close()

# ============ Modelo ============
# Se sua tabela tiver acento no nome, defina TABLE_NAME via env (ex.: "solicitações")
TABLE_NAME = os.getenv('TABLE_NAME', 'solicitacoes')

class Solicitacao(db.Model):
    __tablename__ = quoted_name(TABLE_NAME, True)
    __table_args__ = {'schema': DB_SCHEMA}

    id = db.Column(db.Integer, primary_key=True)
    rua = db.Column(db.String(255), nullable=False)     # mapeia campo nome_rua do form
    numero = db.Column(db.String(50), nullable=False)
    bairro = db.Column(db.String(255), nullable=False)
    situacoes = db.Column(db.Text, nullable=False)      # CSV: "buraco,iluminacao"

# ============ Rotas ============
@app.route('/')
def index():
    # No templates/formulario.html:
    #   <meta charset="utf-8"> e <form ... accept-charset="UTF-8">
    return render_template('formulario.html')

@app.route('/enviar', methods=['POST'])
def enviar_formulario():
    nome_rua = request.form.get('nome_rua')
    numero = request.form.get('numero')
    bairro = request.form.get('bairro')
    situacoes_list = request.form.getlist('situacao')  # múltiplas checkboxes

    if not nome_rua or not numero or not bairro or not situacoes_list:
        flash('Por favor, preencha todos os campos obrigatorios.', 'error')
        return redirect(url_for('index'))

    situacoes_str = ','.join(situacoes_list)

    try:
        nova = Solicitacao(
            rua=nome_rua,
            numero=str(numero),
            bairro=bairro,
            situacoes=situacoes_str
        )
        db.session.add(nova)
        db.session.commit()
        flash('Solicitacao enviada com sucesso!', 'success')
    except Exception:
        db.session.rollback()
        # Log completo no console (evita problemas de encoding na UI)
        traceback.print_exc()
        flash('Erro ao salvar no banco (veja o console para detalhes).', 'error')

    return redirect(url_for('index'))

@app.route('/lista')
def lista():
    itens = Solicitacao.query.order_by(Solicitacao.id.desc()).all()
    linhas = [f"{i.id} - {i.rua}, {i.numero} - {i.bairro} | situacoes: {i.situacoes}" for i in itens]
    return '<br>'.join(linhas) or 'Sem registros.'

# Diagnostico de encoding
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
    app.run(debug=True)